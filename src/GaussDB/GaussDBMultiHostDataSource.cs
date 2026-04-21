using HuaweiCloud.GaussDB.Internal;
using HuaweiCloud.GaussDB.Util;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;

namespace HuaweiCloud.GaussDB;

/// <summary>
/// An <see cref="GaussDBDataSource" /> which manages connections for multiple hosts, is aware of their states (primary, secondary,
/// offline...) and can perform failover and load balancing across them.
/// </summary>
/// <remarks>
/// See <see href="https://www.gaussdb.org/doc/failover-and-load-balancing.html" />.
/// </remarks>
public sealed class GaussDBMultiHostDataSource : GaussDBDataSource
{
    internal override bool OwnsConnectors => false;

    readonly GaussDBDataSource[] _pools;
    readonly ConcurrentDictionary<string, GaussDBDataSource> _endpointPools = new(StringComparer.Ordinal);
    readonly SeedCluster[] _seedClusters;
    readonly string _urlKey;
    readonly ConcurrentDictionary<string, SeedBinding> _seedBindings = new(StringComparer.Ordinal);
    readonly ConcurrentDictionary<string, LogicalNodeRecord> _logicalNodes = new(StringComparer.Ordinal);

    internal GaussDBDataSource[] Pools => _pools;

    readonly MultiHostDataSourceWrapper[] _wrappers;

    volatile int _roundRobinIndex = -1;
    int _logicalNodeOrder;

    internal GaussDBMultiHostDataSource(GaussDBConnectionStringBuilder settings, GaussDBDataSourceConfiguration dataSourceConfig)
        : base(settings, dataSourceConfig)
    {
        var hosts = settings.Host!.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        _pools = new GaussDBDataSource[hosts.Length];
        var seedEndpoints = new HaEndpoint[hosts.Length];
        for (var i = 0; i < hosts.Length; i++)
        {
            var poolSettings = settings.Clone();
            Debug.Assert(!poolSettings.Multiplexing);
            var host = hosts[i].AsSpan().Trim();
            if (GaussDBConnectionStringBuilder.TrySplitHostPort(host, out var newHost, out var newPort))
            {
                poolSettings.Host = newHost;
                poolSettings.Port = newPort;
                seedEndpoints[i] = new(newHost, newPort);
            }
            else
            {
                poolSettings.Host = host.ToString();
                seedEndpoints[i] = new(host.ToString(), settings.Port);
            }

            _pools[i] = settings.Pooling
                ? new PoolingDataSource(poolSettings, dataSourceConfig)
                : new UnpooledDataSource(poolSettings, dataSourceConfig);
            _endpointPools.TryAdd(seedEndpoints[i].Key, _pools[i]);
        }

        _seedClusters = BuildSeedClusters(settings, seedEndpoints);
        _urlKey = CreateKey(seedEndpoints);
        _logicalNodeOrder = seedEndpoints.Length;

        var targetSessionAttributeValues = Enum.GetValues<TargetSessionAttributes>().ToArray();
        var highestValue = 0;
        foreach (var value in targetSessionAttributeValues)
            if ((int)value > highestValue)
                highestValue = (int)value;

        _wrappers = new MultiHostDataSourceWrapper[highestValue + 1];
        foreach (var targetSessionAttribute in targetSessionAttributeValues)
            _wrappers[(int)targetSessionAttribute] = new(this, targetSessionAttribute);
    }

    /// <summary>
    /// Returns a new, unopened connection from this data source.
    /// </summary>
    /// <param name="targetSessionAttributes">Specifies the server type (e.g. primary, standby).</param>
    public GaussDBConnection CreateConnection(TargetSessionAttributes targetSessionAttributes)
        => GaussDBConnection.FromDataSource(_wrappers[(int)targetSessionAttributes]);

    /// <summary>
    /// Returns a new, opened connection from this data source.
    /// </summary>
    /// <param name="targetSessionAttributes">Specifies the server type (e.g. primary, standby).</param>
    public GaussDBConnection OpenConnection(TargetSessionAttributes targetSessionAttributes)
    {
        var connection = CreateConnection(targetSessionAttributes);

        try
        {
            connection.Open();
            return connection;
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Returns a new, opened connection from this data source.
    /// </summary>
    /// <param name="targetSessionAttributes">Specifies the server type (e.g. primary, standby).</param>
    /// <param name="cancellationToken">
    /// An optional token to cancel the asynchronous operation. The default value is <see cref="CancellationToken.None"/>.
    /// </param>
    public async ValueTask<GaussDBConnection> OpenConnectionAsync(
        TargetSessionAttributes targetSessionAttributes,
        CancellationToken cancellationToken = default)
    {
        var connection = CreateConnection(targetSessionAttributes);

        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Returns an <see cref="GaussDBDataSource" /> that wraps this multi-host one with the given server type.
    /// </summary>
    /// <param name="targetSessionAttributes">Specifies the server type (e.g. primary, standby).</param>
    public GaussDBDataSource WithTargetSession(TargetSessionAttributes targetSessionAttributes)
        => _wrappers[(int)targetSessionAttributes];

    static bool IsPreferred(DatabaseState state, TargetSessionAttributes preferredType)
        => state switch
        {
            DatabaseState.Offline => false,
            DatabaseState.Unknown => true, // We will check compatibility again after refreshing the database state

            DatabaseState.PrimaryReadWrite when preferredType is
                TargetSessionAttributes.Primary or
                TargetSessionAttributes.PreferPrimary or
                TargetSessionAttributes.ReadWrite
                => true,

            DatabaseState.PrimaryReadOnly when preferredType is
                TargetSessionAttributes.Primary or
                TargetSessionAttributes.PreferPrimary or
                TargetSessionAttributes.ReadOnly
                => true,

            DatabaseState.Standby when preferredType is
                TargetSessionAttributes.Standby or
                TargetSessionAttributes.PreferStandby or
                TargetSessionAttributes.ReadOnly
                => true,

            _ => preferredType == TargetSessionAttributes.Any
        };

    static bool IsOnline(DatabaseState state, TargetSessionAttributes preferredType)
    {
        Debug.Assert(preferredType is TargetSessionAttributes.PreferPrimary or TargetSessionAttributes.PreferStandby);
        return state != DatabaseState.Offline;
    }

    static bool IsPreferredOrOffline(DatabaseState state, TargetSessionAttributes preferredType)
        => state == DatabaseState.Offline || IsPreferred(state, preferredType);

    static bool IsOnlineOrOffline(DatabaseState state, TargetSessionAttributes preferredType)
        => state == DatabaseState.Offline || IsOnline(state, preferredType);

    async ValueTask<GaussDBConnector?> TryGetIdleOrNew(
        IReadOnlyList<GaussDBDataSource> pools,
        GaussDBConnection conn,
        TimeSpan timeoutPerHost,
        bool async,
        TargetSessionAttributes preferredType, Func<DatabaseState, TargetSessionAttributes, bool> stateValidator,
        IList<Exception> exceptions,
        CancellationToken cancellationToken)
    {
        for (var i = 0; i < pools.Count; i++)
        {
            var pool = pools[i];
            var databaseState = pool.GetDatabaseState();
            if (!stateValidator(databaseState, preferredType))
                continue;

            GaussDBConnector? connector = null;

            try
            {
                if (pool.TryGetIdleConnector(out connector))
                {
                    if (databaseState == DatabaseState.Unknown)
                    {
                        databaseState = await connector.QueryDatabaseState(new GaussDBTimeout(timeoutPerHost), async, cancellationToken).ConfigureAwait(false);
                        Debug.Assert(databaseState != DatabaseState.Unknown);
                        if (!stateValidator(databaseState, preferredType))
                        {
                            pool.Return(connector);
                            continue;
                        }
                    }

                    return connector;
                }
                else
                {
                    connector = await pool.OpenNewConnector(conn, new GaussDBTimeout(timeoutPerHost), async, cancellationToken).ConfigureAwait(false);
                    if (connector is not null)
                    {
                        if (databaseState == DatabaseState.Unknown)
                        {
                            // While opening a new connector we might have refreshed the database state, check again
                            databaseState = pool.GetDatabaseState();
                            if (databaseState == DatabaseState.Unknown)
                                databaseState = await connector.QueryDatabaseState(new GaussDBTimeout(timeoutPerHost), async, cancellationToken).ConfigureAwait(false);
                            Debug.Assert(databaseState != DatabaseState.Unknown);
                            if (!stateValidator(databaseState, preferredType))
                            {
                                pool.Return(connector);
                                continue;
                            }
                        }

                        return connector;
                    }
                }
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
                if (connector is not null)
                    pool.Return(connector);
            }
        }

        return null;
    }

    async ValueTask<GaussDBConnector?> TryGet(
        IReadOnlyList<GaussDBDataSource> pools,
        GaussDBConnection conn,
        TimeSpan timeoutPerHost,
        bool async,
        TargetSessionAttributes preferredType,
        Func<DatabaseState, TargetSessionAttributes, bool> stateValidator,
        IList<Exception> exceptions,
        CancellationToken cancellationToken)
    {
        for (var i = 0; i < pools.Count; i++)
        {
            var pool = pools[i];
            var databaseState = pool.GetDatabaseState();
            if (!stateValidator(databaseState, preferredType))
                continue;

            GaussDBConnector? connector = null;

            try
            {
                connector = await pool.Get(conn, new GaussDBTimeout(timeoutPerHost), async, cancellationToken).ConfigureAwait(false);
                if (databaseState == DatabaseState.Unknown)
                {
                    // Get might have opened a new physical connection and refreshed the database state, check again
                    databaseState = pool.GetDatabaseState();
                    if (databaseState == DatabaseState.Unknown)
                        databaseState = await connector.QueryDatabaseState(new GaussDBTimeout(timeoutPerHost), async, cancellationToken).ConfigureAwait(false);

                    Debug.Assert(databaseState != DatabaseState.Unknown);
                    if (!stateValidator(databaseState, preferredType))
                    {
                        pool.Return(connector);
                        continue;
                    }
                }

                return connector;
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
                if (connector is not null)
                    pool.Return(connector);
            }
        }

        return null;
    }

    internal override async ValueTask<GaussDBConnector> Get(
        GaussDBConnection conn,
        GaussDBTimeout timeout,
        bool async,
        CancellationToken cancellationToken)
    {
        CheckDisposed();

        var exceptions = new List<Exception>();
        var timeoutPerHost = timeout.IsSet ? timeout.CheckAndGetTimeLeft() : TimeSpan.Zero;
        var preferredType = GetTargetSessionAttributes(conn);
        var checkUnpreferred = preferredType is TargetSessionAttributes.PreferPrimary or TargetSessionAttributes.PreferStandby;

        if (!Settings.UsesHaRouting)
        {
            var candidatePools = GetLegacyPools(conn.Settings.LoadBalanceHosts);
            var legacyConnector = await TryConnectPools(
                    candidatePools,
                    conn,
                    timeoutPerHost,
                    async,
                    preferredType,
                    checkUnpreferred,
                    exceptions,
                    cancellationToken)
                .ConfigureAwait(false);

            if (legacyConnector is null && candidatePools.All(static pool => pool.GetDatabaseState() == DatabaseState.Offline))
            {
                // 对齐 JDBC：如果状态缓存把所有 host 都过滤空了，就忽略 Offline 再探测一轮，
                // 让刚恢复的节点有机会在本次连接里被重新命中，而不是死等 HostRecheckSeconds 到期。
                legacyConnector = await TryConnectPools(
                        candidatePools,
                        conn,
                        timeoutPerHost,
                        async,
                        preferredType,
                        checkUnpreferred,
                        exceptions,
                        cancellationToken,
                        allowOffline: true)
                    .ConfigureAwait(false);
            }

            return legacyConnector ?? throw NoSuitableHostsException(exceptions);
        }

        var clusterPlans = await BuildClusterRoutingPlans(conn, async, cancellationToken).ConfigureAwait(false);
        foreach (var clusterPlan in clusterPlans)
        {
            if (clusterPlan.Pools.Length == 0)
                continue;

            var connector = await TryConnectPools(
                    clusterPlan.Pools,
                    conn,
                    timeoutPerHost,
                    async,
                    preferredType,
                    checkUnpreferred,
                    exceptions,
                    cancellationToken,
                    allowOffline: clusterPlan.AllowOffline)
                .ConfigureAwait(false);

            if (connector is not null)
            {
                if (Settings.PriorityServers > 0 && ShouldReportPrimaryCluster(connector))
                    GaussDBGlobalClusterStatusTracker.ReportPrimary(_urlKey, clusterPlan.ClusterKey);

                return connector;
            }
        }

        throw NoSuitableHostsException(exceptions);
    }

    async ValueTask<GaussDBConnector?> TryConnectPools(
        IReadOnlyList<GaussDBDataSource> pools,
        GaussDBConnection conn,
        TimeSpan timeoutPerHost,
        bool async,
        TargetSessionAttributes preferredType,
        bool checkUnpreferred,
        IList<Exception> exceptions,
        CancellationToken cancellationToken,
        bool allowOffline = false)
    {
        Func<DatabaseState, TargetSessionAttributes, bool> preferredValidator = allowOffline ? IsPreferredOrOffline : IsPreferred;
        Func<DatabaseState, TargetSessionAttributes, bool> onlineValidator = allowOffline ? IsOnlineOrOffline : IsOnline;

        return await TryGetIdleOrNew(pools, conn, timeoutPerHost, async, preferredType, preferredValidator, exceptions, cancellationToken).ConfigureAwait(false) ??
               (checkUnpreferred
                   ? await TryGetIdleOrNew(pools, conn, timeoutPerHost, async, preferredType, onlineValidator, exceptions, cancellationToken).ConfigureAwait(false)
                   : null) ??
               await TryGet(pools, conn, timeoutPerHost, async, preferredType, preferredValidator, exceptions, cancellationToken).ConfigureAwait(false) ??
               (checkUnpreferred
                   ? await TryGet(pools, conn, timeoutPerHost, async, preferredType, onlineValidator, exceptions, cancellationToken).ConfigureAwait(false)
                   : null);
    }

    GaussDBDataSource[] GetLegacyPools(bool loadBalanceHosts)
    {
        if (!loadBalanceHosts || _pools.Length <= 1)
            return _pools;

        var startIndex = GetRoundRobinIndex() % _pools.Length;
        var candidatePools = new GaussDBDataSource[_pools.Length];
        for (var i = 0; i < _pools.Length; i++)
            candidatePools[i] = _pools[(startIndex + i) % _pools.Length];

        return candidatePools;
    }

    ValueTask<ClusterRoutingPlan[]> BuildClusterRoutingPlans(GaussDBConnection conn, bool async, CancellationToken cancellationToken)
        => async
            ? BuildClusterRoutingPlansAsync(conn, cancellationToken)
            : new(BuildClusterRoutingPlansAsync(conn, cancellationToken).GetAwaiter().GetResult());

    async ValueTask<ClusterRoutingPlan[]> BuildClusterRoutingPlansAsync(GaussDBConnection conn, CancellationToken cancellationToken)
    {
        // 先确定簇顺序，再在每个簇内决定 CN 顺序；这样 PriorityServers 和 AutoBalance 的职责是分层的。
        var orderedClusters = OrderClusters();
        var preferredClusterKey = Settings.PriorityServers > 0
            ? GaussDBGlobalClusterStatusTracker.GetPreferredClusterKey(_urlKey)
            : null;
        var filteredPlans = new List<ClusterRoutingPlan>(orderedClusters.Length * 2);
        var fallbackPlans = new List<ClusterRoutingPlan>(orderedClusters.Length * 2);
        var hasFilteredCandidate = false;
        foreach (var cluster in orderedClusters)
        {
            var endpoints = await ResolveClusterEndpoints(cluster, preferredClusterKey, cancellationToken).ConfigureAwait(false);
            var clusterPools = endpoints.Select(GetOrAddEndpointPool).ToArray();
            var filteredClusterPools = clusterPools
                .Where(static pool => pool.GetDatabaseState() != DatabaseState.Offline)
                .ToArray();
            filteredPlans.Add(new(cluster.Key, filteredClusterPools, AllowOffline: false));
            fallbackPlans.Add(new(cluster.Key, clusterPools, AllowOffline: true));
            hasFilteredCandidate |= filteredClusterPools.Length > 0;

            if (ShouldAppendSeedFallbackPlan(cluster, endpoints))
            {
                var seedFallbackPools = cluster.SeedEndpoints.Select(GetOrAddEndpointPool).ToArray();
                var filteredSeedFallbackPools = seedFallbackPools
                    .Where(static pool => pool.GetDatabaseState() != DatabaseState.Offline)
                    .ToArray();
                filteredPlans.Add(new(cluster.Key, filteredSeedFallbackPools, AllowOffline: false));
                fallbackPlans.Add(new(cluster.Key, seedFallbackPools, AllowOffline: true));
                hasFilteredCandidate |= filteredSeedFallbackPools.Length > 0;
            }
        }

        if (hasFilteredCandidate)
            return filteredPlans.ToArray();

        // 对齐 JDBC：如果状态缓存把整轮候选都裁空了，则按原有簇顺序忽略 Offline 再扫一遍。
        return fallbackPlans.ToArray();
    }

    SeedCluster[] OrderClusters()
    {
        if (_seedClusters.Length <= 1)
            return _seedClusters;

        // 一旦某个簇成功提供了主库连接，就把它记成当前已知主簇，后续优先回到这个簇。
        var preferredClusterKey = GaussDBGlobalClusterStatusTracker.GetPreferredClusterKey(_urlKey);
        if (preferredClusterKey is null)
            return _seedClusters;

        return _seedClusters
            .OrderByDescending(cluster => cluster.Key == preferredClusterKey)
            .ToArray();
    }

    async ValueTask<HaEndpoint[]> ResolveClusterEndpoints(SeedCluster cluster, string? preferredClusterKey, CancellationToken cancellationToken)
    {
        if (!ShouldUseCoordinatorSnapshot(cluster.Key, preferredClusterKey))
            return BuildLogicalCandidates(cluster, includeUnboundSeedEndpoints: true);

        var snapshot = await GaussDBCoordinatorListTracker.GetSnapshotAsync(
                cluster.Key,
                TimeSpan.FromSeconds(Settings.RefreshCNIpListTime),
                ct => RefreshCoordinatorEndpoints(cluster, ct),
                async: true,
                cancellationToken)
            .ConfigureAwait(false);

        if (snapshot is { Length: > 0 })
        {
            await EnsureSeedBindingsAsync(snapshot, cancellationToken).ConfigureAwait(false);
            MergeDiscoveredNodes(cluster, snapshot);
        }

        return BuildLogicalCandidates(cluster, includeUnboundSeedEndpoints: false);
    }

    bool ShouldUseCoordinatorSnapshot(string clusterKey, string? preferredClusterKey)
    {
        if (Settings.RefreshCNIpListTime <= 0)
            return false;

        if (Settings.AutoBalanceModeParsed == HaAutoBalanceMode.Disabled)
            return false;

        if (Settings.PriorityServers == 0)
            return true;

        return preferredClusterKey is not null && clusterKey == preferredClusterKey;
    }

    bool ShouldAppendSeedFallbackPlan(SeedCluster cluster, IReadOnlyList<HaEndpoint> resolvedEndpoints)
    {
        if (Settings.PriorityServers > 0 || Settings.AutoBalanceModeParsed == HaAutoBalanceMode.Disabled)
            return false;

        foreach (var seed in cluster.SeedEndpoints)
            if (!resolvedEndpoints.Any(endpoint => endpoint.Key == seed.Key))
                return true;

        return false;
    }

    async ValueTask<HaCoordinatorNode[]?> RefreshCoordinatorEndpoints(SeedCluster cluster, CancellationToken cancellationToken)
    {
        // 只要该簇里有一个可达 seed CN，就用它查询 pgxc_node，拿到当前有效的 CN 列表。
        foreach (var endpoint in cluster.SeedEndpoints)
        {
            var pool = GetOrAddEndpointPool(endpoint);
            try
            {
                var connection = await pool.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
                await using (connection.ConfigureAwait(false))
                {
                    using var command = connection.CreateCommand();
                    command.CommandText =
                        "select node_name,node_host,node_port,node_host1,node_port1 " +
                        "from pgxc_node where node_type='C' and nodeis_active = true order by node_name;";

                    var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                    await using (reader.ConfigureAwait(false))
                    {
                        var refreshedNodes = new List<HaCoordinatorNode>();
                        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                            refreshedNodes.Add(new(
                                reader.GetString(0),
                                new HaEndpoint(reader.GetString(1), reader.GetInt32(2)),
                                new HaEndpoint(reader.GetString(3), reader.GetInt32(4))));

                        return refreshedNodes.Count == 0 ? null : refreshedNodes.ToArray();
                    }
                }
            }
            catch
            {
                // Fall back to static seed routing if discovery is unavailable from this endpoint.
            }
        }

        return null;
    }

    async ValueTask EnsureSeedBindingsAsync(IReadOnlyList<HaCoordinatorNode> snapshot, CancellationToken cancellationToken)
    {
        foreach (var cluster in _seedClusters)
        {
            for (var i = 0; i < cluster.SeedEndpoints.Length; i++)
            {
                var seedEndpoint = cluster.SeedEndpoints[i];
                if (_seedBindings.ContainsKey(seedEndpoint.Key))
                    continue;

                var match = FindUniqueMatchingNode(snapshot, seedEndpoint) ??
                            await IdentifySeedNodeAsync(seedEndpoint, snapshot, cancellationToken).ConfigureAwait(false);
                if (match is null)
                    continue;

                TryBindSeedEndpoint(cluster, i, seedEndpoint, match.Value.NodeName);
            }
        }
    }

    async ValueTask<HaCoordinatorNode?> IdentifySeedNodeAsync(
        HaEndpoint seedEndpoint,
        IReadOnlyList<HaCoordinatorNode> snapshot,
        CancellationToken cancellationToken)
    {
        var probeSettings = Settings.Clone();
        probeSettings.Host = seedEndpoint.Host;
        probeSettings.Port = seedEndpoint.Port;
        probeSettings.Pooling = false;
        probeSettings.LoadBalanceHosts = false;
        probeSettings.Remove(nameof(GaussDBConnectionStringBuilder.PriorityServers));
        probeSettings.Remove(nameof(GaussDBConnectionStringBuilder.AutoBalance));
        probeSettings.Remove(nameof(GaussDBConnectionStringBuilder.RefreshCNIpListTime));
        probeSettings.Timeout = probeSettings.Timeout == 0 ? 2 : Math.Min(probeSettings.Timeout, 2);
        probeSettings.CommandTimeout = probeSettings.CommandTimeout == 0 ? 2 : Math.Min(probeSettings.CommandTimeout, 2);

        try
        {
            var connection = new GaussDBConnection(probeSettings.ConnectionString);
            await using (connection.ConfigureAwait(false))
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                using var command = connection.CreateCommand();
                command.CommandText = "select get_nodename();";

                var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                        return null;

                    var nodeName = reader.GetString(0);
                    for (var i = 0; i < snapshot.Count; i++)
                    {
                        var discovered = snapshot[i];
                        if (discovered.NodeName == nodeName)
                            return discovered;
                    }

                    return null;
                }
            }
        }
        catch
        {
            return null;
        }
    }

    static HaCoordinatorNode? FindUniqueMatchingNode(IReadOnlyList<HaCoordinatorNode> snapshot, HaEndpoint endpoint)
    {
        HaCoordinatorNode? match = null;
        for (var i = 0; i < snapshot.Count; i++)
        {
            var discovered = snapshot[i];
            if (!discovered.Matches(endpoint))
                continue;

            if (match is not null)
                return null;

            match = discovered;
        }

        return match;
    }

    void TryBindSeedEndpoint(SeedCluster cluster, int seedIndex, HaEndpoint seedEndpoint, string nodeName)
    {
        if (_logicalNodes.TryGetValue(nodeName, out var existing) && existing.ClusterKey != cluster.Key)
            return;

        var seedOrder = GetSeedOrder(cluster, seedIndex);
        var record = _logicalNodes.GetOrAdd(
            nodeName,
            static (name, state) => new LogicalNodeRecord(name, state.ClusterKey, state.SeedEndpoint, state.SeedOrder),
            (ClusterKey: cluster.Key, SeedEndpoint: seedEndpoint, SeedOrder: seedOrder));

        if (record.ClusterKey != cluster.Key)
            return;

        record.SetSeedEndpoint(seedEndpoint, seedOrder);
        _seedBindings.TryAdd(seedEndpoint.Key, new SeedBinding(seedEndpoint, nodeName, cluster.Key, seedOrder));
    }

    void MergeDiscoveredNodes(SeedCluster cluster, IReadOnlyList<HaCoordinatorNode> snapshot)
    {
        if (!ClusterHasConfirmedSeedBinding(cluster))
            return;

        for (var i = 0; i < snapshot.Count; i++)
        {
            var discovered = snapshot[i];
            var preferredEndpoint = discovered.GetPreferredEndpoint(Settings.UsingEip);
            if (_logicalNodes.TryGetValue(discovered.NodeName, out var existing))
            {
                existing.UpdateDynamicEndpoint(preferredEndpoint);
                continue;
            }

            var order = Interlocked.Increment(ref _logicalNodeOrder);
            var created = new LogicalNodeRecord(discovered.NodeName, cluster.Key, null, order);
            created.UpdateDynamicEndpoint(preferredEndpoint);
            _logicalNodes.TryAdd(discovered.NodeName, created);
        }
    }

    bool ClusterHasConfirmedSeedBinding(SeedCluster cluster)
        => cluster.SeedEndpoints.Any(seed => _seedBindings.ContainsKey(seed.Key));

    HaEndpoint[] BuildLogicalCandidates(SeedCluster cluster, bool includeUnboundSeedEndpoints)
    {
        var candidates = new List<LogicalNodeCandidate>(cluster.SeedEndpoints.Length + _logicalNodes.Count);
        var seenNodeNames = new HashSet<string>(StringComparer.Ordinal);

        for (var i = 0; i < cluster.SeedEndpoints.Length; i++)
        {
            var seedEndpoint = cluster.SeedEndpoints[i];
            if (_seedBindings.TryGetValue(seedEndpoint.Key, out var binding) &&
                _logicalNodes.TryGetValue(binding.NodeName, out var node) &&
                node.ClusterKey == cluster.Key)
            {
                candidates.Add(new(node.NodeName, node.SeedEndpoint, node.GetDynamicEndpoint(), node.Order));
                seenNodeNames.Add(node.NodeName);
                continue;
            }

            if (includeUnboundSeedEndpoints)
                candidates.Add(new(null, seedEndpoint, null, GetSeedOrder(cluster, i)));
        }

        foreach (var node in _logicalNodes.Values
                     .Where(node => node.ClusterKey == cluster.Key && !seenNodeNames.Contains(node.NodeName))
                     .OrderBy(node => node.Order))
        {
            candidates.Add(new(node.NodeName, node.SeedEndpoint, node.GetDynamicEndpoint(), node.Order));
        }

        var orderedCandidates = OrderClusterEndpoints(Settings, cluster, candidates);
        var flattened = new List<HaEndpoint>(orderedCandidates.Length * 2);
        var seenEndpoints = new HashSet<string>(StringComparer.Ordinal);
        foreach (var candidate in orderedCandidates)
        {
            var dynamicEndpoint = candidate.DynamicEndpoint;
            if (dynamicEndpoint is not null && seenEndpoints.Add(dynamicEndpoint.Value.Key))
                flattened.Add(dynamicEndpoint.Value);

            var seedEndpoint = candidate.SeedEndpoint;
            if (seedEndpoint is not null && seenEndpoints.Add(seedEndpoint.Value.Key))
                flattened.Add(seedEndpoint.Value);
        }

        if (flattened.Count > 0)
            return flattened.ToArray();

        return includeUnboundSeedEndpoints
            ? cluster.SeedEndpoints
            : [];
    }

    LogicalNodeCandidate[] OrderClusterEndpoints(
        GaussDBConnectionStringBuilder settings,
        SeedCluster cluster,
        IReadOnlyList<LogicalNodeCandidate> resolvedEndpoints)
    {
        if (resolvedEndpoints.Count <= 1)
            return resolvedEndpoints.ToArray();

        var allEndpoints = resolvedEndpoints.ToList();
        var priorityCount = settings.GetEffectivePriorityHostCount(cluster.SeedEndpoints.Length);

        List<LogicalNodeCandidate> priorityEndpoints;
        List<LogicalNodeCandidate> nonPriorityEndpoints;
        if (priorityCount > 0)
        {
            var priorityKeys = new HashSet<string>(cluster.SeedEndpoints.Take(priorityCount).Select(static endpoint => endpoint.Key), StringComparer.Ordinal);
            priorityEndpoints = allEndpoints.Where(endpoint => endpoint.SeedEndpoint is not null && priorityKeys.Contains(endpoint.SeedEndpoint.Value.Key)).ToList();
            nonPriorityEndpoints = allEndpoints.Where(endpoint => endpoint.SeedEndpoint is null || !priorityKeys.Contains(endpoint.SeedEndpoint.Value.Key)).ToList();
        }
        else
        {
            priorityEndpoints = [];
            nonPriorityEndpoints = allEndpoints;
        }

        // AutoBalance 只作用在“当前选中的簇”内部，不改变簇级优先级。
        switch (settings.AutoBalanceModeParsed)
        {
        case HaAutoBalanceMode.Shuffle:
            Shuffle(nonPriorityEndpoints);
            return nonPriorityEndpoints.ToArray();
        case HaAutoBalanceMode.RoundRobin:
            return Rotate(nonPriorityEndpoints).ToArray();
        case HaAutoBalanceMode.Priority:
            if (priorityEndpoints.Count > 0)
            {
                var orderedPriorityEndpoints = Rotate(priorityEndpoints);
                Shuffle(nonPriorityEndpoints);
                orderedPriorityEndpoints.AddRange(nonPriorityEndpoints);
                return orderedPriorityEndpoints.ToArray();
            }

            return Rotate(allEndpoints).ToArray();
        case HaAutoBalanceMode.ShufflePriority:
            if (priorityEndpoints.Count > 0)
            {
                Shuffle(priorityEndpoints);
                Shuffle(nonPriorityEndpoints);
                priorityEndpoints.AddRange(nonPriorityEndpoints);
                return priorityEndpoints.ToArray();
            }

            return Rotate(allEndpoints).ToArray();
        default:
            return settings.LoadBalanceHosts
                ? Rotate(allEndpoints).ToArray()
                : allEndpoints.ToArray();
        }
    }

    static int GetSeedOrder(SeedCluster cluster, int index)
        => index;

    GaussDBDataSource GetOrAddEndpointPool(HaEndpoint endpoint)
        => _endpointPools.GetOrAdd(endpoint.Key, _ =>
        {
            var poolSettings = Settings.Clone();
            poolSettings.Host = endpoint.Host;
            poolSettings.Port = endpoint.Port;
            return Settings.Pooling
                ? new PoolingDataSource(poolSettings, Configuration)
                : new UnpooledDataSource(poolSettings, Configuration);
        });

    List<T> Rotate<T>(IReadOnlyList<T> source)
    {
        if (source.Count <= 1)
            return source.ToList();

        var startIndex = GetRoundRobinIndex() % source.Count;
        var rotated = new List<T>(source.Count);
        for (var i = 0; i < source.Count; i++)
            rotated.Add(source[(startIndex + i) % source.Count]);
        return rotated;
    }

    static void Shuffle<T>(IList<T> values)
    {
        for (var i = values.Count - 1; i > 0; i--)
        {
            var swapIndex = Random.Shared.Next(i + 1);
            (values[i], values[swapIndex]) = (values[swapIndex], values[i]);
        }
    }

    static GaussDBException NoSuitableHostsException(IList<Exception> exceptions)
    {
        return exceptions.Count == 0
            ? new GaussDBException("No suitable host was found.")
            : exceptions[0] is PostgresException firstException && AllEqual(firstException, exceptions)
                ? firstException
                : new GaussDBException("Unable to connect to a suitable host. Check inner exception for more details.",
                    new AggregateException(exceptions));

        static bool AllEqual(PostgresException first, IList<Exception> exceptions)
        {
            foreach (var x in exceptions)
                if (x is not PostgresException ex || ex.SqlState != first.SqlState)
                    return false;
            return true;
        }
    }

    int GetRoundRobinIndex()
    {
        while (true)
        {
            var index = Interlocked.Increment(ref _roundRobinIndex);
            if (index >= 0)
                return index % _pools.Length;

            // Worst case scenario - we've wrapped around integer counter
            if (index == int.MinValue)
            {
                // This is the thread which wrapped around the counter - reset it to 0
                _roundRobinIndex = 0;
                return 0;
            }

            // This is not the thread which wrapped around the counter - just wait until it's 0 or more
            var sw = new SpinWait();
            while (_roundRobinIndex < 0)
                sw.SpinOnce();
        }
    }

    internal override void Return(GaussDBConnector connector)
        => throw new GaussDBException("GaussDB bug: a connector was returned to " + nameof(GaussDBMultiHostDataSource));

    internal override bool TryGetIdleConnector([NotNullWhen(true)] out GaussDBConnector? connector)
        => throw new GaussDBException("GaussDB bug: trying to get an idle connector from " + nameof(GaussDBMultiHostDataSource));

    internal override ValueTask<GaussDBConnector?> OpenNewConnector(GaussDBConnection conn, GaussDBTimeout timeout, bool async, CancellationToken cancellationToken)
        => throw new GaussDBException("GaussDB bug: trying to open a new connector from " + nameof(GaussDBMultiHostDataSource));

    /// <inheritdoc />
    public override void Clear()
    {
        foreach (var pool in _endpointPools.Values)
            pool.Clear();
    }

    /// <summary>
    /// Clears the database state (primary, secondary, offline...) for all data sources managed by this multi-host data source.
    /// Can be useful to make GaussDB retry a PostgreSQL instance which was previously detected to be offline.
    /// </summary>
    public void ClearDatabaseStates()
    {
        foreach (var pool in _endpointPools.Values)
        {
            pool.UpdateDatabaseState(default, default, default, ignoreTimeStamp: true);
        }
    }

    internal override (int Total, int Idle, int Busy) Statistics
    {
        get
        {
            var numConnectors = 0;
            var idleCount = 0;

            foreach (var pool in _endpointPools.Values)
            {
                var stat = pool.Statistics;
                numConnectors += stat.Total;
                idleCount += stat.Idle;
            }

            return (numConnectors, idleCount, numConnectors - idleCount);
        }
    }

    internal override bool TryRentEnlistedPending(
        Transaction transaction,
        GaussDBConnection connection,
        [NotNullWhen(true)] out GaussDBConnector? connector)
    {
        lock (_pendingEnlistedConnectors)
        {
            if (!_pendingEnlistedConnectors.TryGetValue(transaction, out var list))
            {
                connector = null;
                return false;
            }

            var preferredType = GetTargetSessionAttributes(connection);
            // First try to get a valid preferred connector.
            if (TryGetValidConnector(list, preferredType, IsPreferred, out connector))
            {
                return true;
            }

            // Can't get valid preferred connector. Try to get an unpreferred connector, if supported.
            if ((preferredType == TargetSessionAttributes.PreferPrimary || preferredType == TargetSessionAttributes.PreferStandby)
                && TryGetValidConnector(list, preferredType, IsOnline, out connector))
            {
                return true;
            }

            connector = null;
            return false;
        }

        bool TryGetValidConnector(List<GaussDBConnector> list, TargetSessionAttributes preferredType,
            Func<DatabaseState, TargetSessionAttributes, bool> validationFunc, [NotNullWhen(true)] out GaussDBConnector? connector)
        {
            for (var i = list.Count - 1; i >= 0; i--)
            {
                connector = list[i];
                var lastKnownState = connector.DataSource.GetDatabaseState(ignoreExpiration: true);
                Debug.Assert(lastKnownState != DatabaseState.Unknown);
                if (validationFunc(lastKnownState, preferredType))
                {
                    list.RemoveAt(i);
                    if (list.Count == 0)
                        _pendingEnlistedConnectors.Remove(transaction);
                    return true;
                }
            }

            connector = null;
            return false;
        }
    }

    static TargetSessionAttributes GetTargetSessionAttributes(GaussDBConnection connection)
        => connection.Settings.TargetSessionAttributesParsed ??
           (PostgresEnvironment.TargetSessionAttributes is { } s
               ? GaussDBConnectionStringBuilder.ParseTargetSessionAttributes(s)
               : TargetSessionAttributes.Any);

    static bool ShouldReportPrimaryCluster(GaussDBConnector connector)
        => connector.DataSource.GetDatabaseState(ignoreExpiration: true) is
            DatabaseState.PrimaryReadWrite or DatabaseState.PrimaryReadOnly;

    static SeedCluster[] BuildSeedClusters(GaussDBConnectionStringBuilder settings, HaEndpoint[] seedEndpoints)
    {
        if (settings.PriorityServers > 0 && settings.PriorityServers < seedEndpoints.Length)
        {
            // 约定 seed 列表前 N 个属于优先 AZ，剩余属于兜底 AZ。
            return
            [
                new(CreateKey(seedEndpoints[..settings.PriorityServers]), seedEndpoints[..settings.PriorityServers]),
                new(CreateKey(seedEndpoints[settings.PriorityServers..]), seedEndpoints[settings.PriorityServers..])
            ];
        }

        return [new(CreateKey(seedEndpoints), seedEndpoints)];
    }

    static string CreateKey(IReadOnlyCollection<HaEndpoint> endpoints)
        => string.Join(",", endpoints.Select(static endpoint => endpoint.Key).OrderBy(static endpoint => endpoint, StringComparer.Ordinal));

    sealed class LogicalNodeRecord
    {
        readonly object _sync = new();
        HaEndpoint? _seedEndpoint;
        HaEndpoint? _dynamicEndpoint;
        int _order;

        internal LogicalNodeRecord(string nodeName, string clusterKey, HaEndpoint? seedEndpoint, int order)
        {
            NodeName = nodeName;
            ClusterKey = clusterKey;
            _seedEndpoint = seedEndpoint;
            _order = order;
        }

        internal string NodeName { get; }
        internal string ClusterKey { get; }

        internal HaEndpoint? SeedEndpoint
        {
            get
            {
                lock (_sync)
                    return _seedEndpoint;
            }
        }

        internal int Order
        {
            get
            {
                lock (_sync)
                    return _order;
            }
        }

        internal void SetSeedEndpoint(HaEndpoint seedEndpoint, int order)
        {
            lock (_sync)
            {
                _seedEndpoint ??= seedEndpoint;
                _order = Math.Min(_order, order);
            }
        }

        internal void UpdateDynamicEndpoint(HaEndpoint endpoint)
        {
            lock (_sync)
                _dynamicEndpoint = endpoint;
        }

        internal HaEndpoint? GetDynamicEndpoint()
        {
            lock (_sync)
                return _dynamicEndpoint;
        }
    }

    readonly record struct SeedBinding(HaEndpoint SeedEndpoint, string NodeName, string ClusterKey, int SeedOrder);
    readonly record struct LogicalNodeCandidate(string? NodeName, HaEndpoint? SeedEndpoint, HaEndpoint? DynamicEndpoint, int Order);
    readonly record struct SeedCluster(string Key, HaEndpoint[] SeedEndpoints);
    readonly record struct ClusterRoutingPlan(string ClusterKey, GaussDBDataSource[] Pools, bool AllowOffline);
}
