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
    internal const string CoordinatorMetadataSource = "pgxc_node";
    internal const string DisasterCoordinatorMetadataSource = "pgxc_disaster_read_node()";
    internal const string DisasterClusterRunModeSql = "select disaster_cluster_run_mode();";

    internal override bool OwnsConnectors => false;

    readonly GaussDBDataSource[] _pools;
    readonly ConcurrentDictionary<string, GaussDBDataSource> _endpointPools = new(StringComparer.Ordinal);
    readonly SeedCluster[] _seedClusters;
    readonly string _urlKey;

    internal GaussDBDataSource[] Pools => _pools;

    readonly MultiHostDataSourceWrapper[] _wrappers;
    static readonly ConcurrentDictionary<string, RoundRobinCounter> RoundRobinCounters = new(StringComparer.Ordinal);

    sealed class RoundRobinCounter
    {
        internal int Value = -1;
    }

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
        // 统一封装一轮 pool 扫描顺序：先空闲/新建，再普通获取；先首选节点，再可接受的非首选节点。
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

        var candidatePools = new GaussDBDataSource[_pools.Length];
        Array.Copy(_pools, candidatePools, _pools.Length);
        Shuffle(candidatePools);
        return candidatePools;
    }

    ValueTask<ClusterRoutingPlan[]> BuildClusterRoutingPlans(GaussDBConnection conn, bool async, CancellationToken cancellationToken)
        => async
            ? BuildClusterRoutingPlansAsync(conn, cancellationToken)
            : new(BuildClusterRoutingPlansAsync(conn, cancellationToken).GetAwaiter().GetResult());

    async ValueTask<ClusterRoutingPlan[]> BuildClusterRoutingPlansAsync(GaussDBConnection conn, CancellationToken cancellationToken)
    {
        // 先确定簇顺序，再在每个簇内决定 CN 顺序；这样 PriorityServers 和 AutoBalance 的职责是分层的。
        var clusterOrder = OrderClusters();
        var preferredClusterKey = Settings.PriorityServers > 0
            ? GaussDBGlobalClusterStatusTracker.GetPreferredClusterKey(_urlKey)
            : null;
        var filteredPlans = new List<ClusterRoutingPlan>(clusterOrder.Length * 2);
        var fallbackPlans = new List<ClusterRoutingPlan>(clusterOrder.Length * 2);
        var hasFilteredCandidate = false;
        foreach (var cluster in clusterOrder)
        {
            var endpoints = await ResolveClusterEndpoints(cluster, preferredClusterKey, cancellationToken).ConfigureAwait(false);
            // 一份计划保留在线节点，另一份保留所有节点，供“整轮都被 Offline 裁空”时兜底。
            var clusterPools = endpoints.Select(GetOrAddEndpointPool).ToArray();
            var filteredClusterPools = clusterPools
                .Where(static pool => pool.GetDatabaseState() != DatabaseState.Offline)
                .ToArray();
            filteredPlans.Add(new(cluster.Key, filteredClusterPools, AllowOffline: false));
            fallbackPlans.Add(new(cluster.Key, clusterPools, AllowOffline: true));
            hasFilteredCandidate |= filteredClusterPools.Length > 0;

            if (ShouldAppendSeedFallbackPlan(cluster, endpoints))
            {
                // 动态 CN 列表没有覆盖全部 seed 时，再补一轮静态 seed fallback。
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
        // 不使用动态快照时，直接基于连接串里的 seed endpoints 做簇内排序。
        if (!ShouldUseCoordinatorSnapshot(cluster.Key, preferredClusterKey))
            return OrderClusterEndpoints(Settings, cluster, cluster.SeedEndpoints);

        // 快照获取带缓存与节流；真正刷新时才会查询 pgxc_node。
        var snapshot = await GaussDBCoordinatorListTracker.GetSnapshotAsync(
                cluster.Key,
                TimeSpan.FromSeconds(Settings.RefreshCNIpListTime),
                ct => RefreshCoordinatorEndpoints(cluster, ct),
                async: true,
                cancellationToken)
            .ConfigureAwait(false);

        return BuildClusterCandidates(cluster, snapshot);
    }

    bool ShouldUseCoordinatorSnapshot(string clusterKey, string? preferredClusterKey)
    {
        // 显式关闭刷新时，直接退回静态 seed 路由。
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
        // PriorityServers 场景下不额外补 seed fallback，避免把非首选簇又提前。
        if (Settings.PriorityServers > 0 || Settings.AutoBalanceModeParsed == HaAutoBalanceMode.Disabled)
            return false;

        // 只要动态候选里缺失任一 seed，就追加一次静态 seed 扫描。
        foreach (var seed in cluster.SeedEndpoints)
            if (!resolvedEndpoints.Any(endpoint => endpoint.Key == seed.Key))
                return true;

        return false;
    }

    async ValueTask<HaEndpoint[]?> RefreshCoordinatorEndpoints(SeedCluster cluster, CancellationToken cancellationToken)
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
                    var refreshSql = await ResolveCoordinatorRefreshSqlAsync(connection, cancellationToken).ConfigureAwait(false);
                    using var command = connection.CreateCommand();
                    command.CommandText = refreshSql;

                    var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                    await using (reader.ConfigureAwait(false))
                    {
                        var refreshedEndpoints = new List<HaEndpoint>();
                        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                            refreshedEndpoints.Add(new(reader.GetString(0), reader.GetInt32(1)));

                        return refreshedEndpoints.Count == 0 ? null : refreshedEndpoints.ToArray();
                    }
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch
            {
                // Fall back to static seed routing if discovery is unavailable from this endpoint.
            }
        }

        return null;
    }

    async ValueTask<string> ResolveCoordinatorRefreshSqlAsync(GaussDBConnection connection, CancellationToken cancellationToken)
    {
        var dataSource = CoordinatorMetadataSource;
        if (Settings.DisasterToleranceCluster)
        {
            using var command = connection.CreateCommand();
            command.CommandText = DisasterClusterRunModeSql;

            var runMode = ParseDisasterClusterRunMode(
                await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
            dataSource = SelectCoordinatorMetadataSource(Settings.DisasterToleranceCluster, runMode);
        }

        return BuildCoordinatorRefreshSql(dataSource, Settings.UsingEip);
    }

    internal static string SelectCoordinatorMetadataSource(bool disasterToleranceCluster, int disasterClusterRunMode)
        => disasterToleranceCluster && disasterClusterRunMode == 1
            ? DisasterCoordinatorMetadataSource
            : CoordinatorMetadataSource;

    internal static int ParseDisasterClusterRunMode(object? value)
        => value is null or DBNull ? int.MinValue : Convert.ToInt32(value);

    internal static string BuildCoordinatorRefreshSql(string dataSource, bool usingEip)
        => usingEip
            ? $"select node_host1,node_port1 from {dataSource} where node_type='C' and nodeis_active = true order by node_host1;"
            : $"select node_host,node_port from {dataSource} where node_type='C' and nodeis_active = true order by node_host;";

    HaEndpoint[] BuildClusterCandidates(SeedCluster cluster, IReadOnlyList<HaEndpoint>? dynamicEndpoints)
    {
        // 动态发现出的 endpoint 优先尝试，但连接串里的 seed endpoints 始终保留作兜底。
        var orderedDynamicEndpoints = dynamicEndpoints is { Count: > 0 }
            ? OrderClusterEndpoints(Settings, cluster, dynamicEndpoints)
            : [];
        var flattened = new List<HaEndpoint>(orderedDynamicEndpoints.Length + cluster.SeedEndpoints.Length);
        var seenEndpoints = new HashSet<string>(StringComparer.Ordinal);

        foreach (var endpoint in orderedDynamicEndpoints)
        {
            if (seenEndpoints.Add(endpoint.Key))
                flattened.Add(endpoint);
        }

        foreach (var seedEndpoint in cluster.SeedEndpoints)
        {
            if (seenEndpoints.Add(seedEndpoint.Key))
                flattened.Add(seedEndpoint);
        }

        return flattened.Count > 0
            ? flattened.ToArray()
            : cluster.SeedEndpoints;
    }

    HaEndpoint[] OrderClusterEndpoints(
        GaussDBConnectionStringBuilder settings,
        SeedCluster cluster,
        IReadOnlyList<HaEndpoint> resolvedEndpoints)
    {
        // 这里只做“簇内排序”；簇与簇之间的优先级已经在 BuildClusterRoutingPlansAsync 里决定。
        if (resolvedEndpoints.Count <= 1)
            return resolvedEndpoints.ToArray();

        var allEndpoints = resolvedEndpoints.ToList();
        var priorityCount = settings.GetEffectivePriorityHostCount(cluster.SeedEndpoints.Length);

        List<HaEndpoint> priorityEndpoints;
        List<HaEndpoint> nonPriorityEndpoints;
        if (priorityCount > 0)
        {
            var priorityKeys = new HashSet<string>(cluster.SeedEndpoints.Take(priorityCount).Select(static endpoint => endpoint.Key), StringComparer.Ordinal);
            priorityEndpoints = allEndpoints.Where(endpoint => priorityKeys.Contains(endpoint.Key)).ToList();
            nonPriorityEndpoints = allEndpoints.Where(endpoint => !priorityKeys.Contains(endpoint.Key)).ToList();
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
            return RoundRobin(nonPriorityEndpoints).ToArray();
        case HaAutoBalanceMode.PriorityRoundRobin:
            if (priorityEndpoints.Count > 0)
            {
                var orderedPriorityEndpoints = RoundRobin(priorityEndpoints);
                Shuffle(nonPriorityEndpoints);
                orderedPriorityEndpoints.AddRange(nonPriorityEndpoints);
                return orderedPriorityEndpoints.ToArray();
            }

            return RoundRobin(allEndpoints).ToArray();
        case HaAutoBalanceMode.PriorityShuffle:
            if (priorityEndpoints.Count > 0)
            {
                Shuffle(priorityEndpoints);
                Shuffle(nonPriorityEndpoints);
                priorityEndpoints.AddRange(nonPriorityEndpoints);
                return priorityEndpoints.ToArray();
            }

            Shuffle(allEndpoints);
            return allEndpoints.ToArray();
        case HaAutoBalanceMode.Specified:
            return RoundRobin(cluster.SeedEndpoints).ToArray();
        case HaAutoBalanceMode.LeastConnection:
            return allEndpoints.ToArray();
        default:
            return settings.LoadBalanceHosts
                ? ShuffleCopy(allEndpoints).ToArray()
                : allEndpoints.ToArray();
        }
    }

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

    List<T> RoundRobin<T>(IReadOnlyList<T> source)
    {
        if (source.Count <= 1)
            return source.ToList();

        var startIndex = GetRoundRobinIndex() % source.Count;
        var rotated = new List<T>(source.Count);
        for (var i = 0; i < source.Count; i++)
            rotated.Add(source[(startIndex + i) % source.Count]);
        if (rotated.Count > 2)
            Shuffle(rotated, startIndex: 1);
        return rotated;
    }

    static List<T> ShuffleCopy<T>(IReadOnlyList<T> source)
    {
        var copy = source.ToList();
        Shuffle(copy);
        return copy;
    }

    static void Shuffle<T>(IList<T> values)
        => Shuffle(values, startIndex: 0);

    static void Shuffle<T>(IList<T> values, int startIndex)
    {
        for (var i = values.Count - 1; i > 0; i--)
        {
            if (i < startIndex)
                break;
            var swapIndex = Random.Shared.Next(i + 1);
            if (swapIndex < startIndex)
                swapIndex = startIndex + Random.Shared.Next(i - startIndex + 1);
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
        var counter = RoundRobinCounters.GetOrAdd(_urlKey, static _ => new RoundRobinCounter());
        while (true)
        {
            var index = Interlocked.Increment(ref counter.Value);
            if (index >= 0)
                return index;

            // Worst case scenario - we've wrapped around integer counter
            if (index == int.MinValue)
            {
                // This is the thread which wrapped around the counter - reset it to 0
                Volatile.Write(ref counter.Value, 0);
                return 0;
            }

            // This is not the thread which wrapped around the counter - just wait until it's 0 or more
            var sw = new SpinWait();
            while (Volatile.Read(ref counter.Value) < 0)
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

    // 一个簇对应一组 seed endpoints；PriorityServers 会把原始 host 列表切成多个簇。
    readonly record struct SeedCluster(string Key, HaEndpoint[] SeedEndpoints);
    // 一轮簇级路由计划，同时说明要扫描哪些 pool，以及是否允许继续尝试 Offline 节点。
    readonly record struct ClusterRoutingPlan(string ClusterKey, GaussDBDataSource[] Pools, bool AllowOffline);
}
