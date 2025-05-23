using HuaweiCloud.GaussDB.Internal;
using HuaweiCloud.GaussDB.Util;
using System;
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

    internal GaussDBDataSource[] Pools => _pools;

    readonly MultiHostDataSourceWrapper[] _wrappers;

    volatile int _roundRobinIndex = -1;

    internal GaussDBMultiHostDataSource(GaussDBConnectionStringBuilder settings, GaussDBDataSourceConfiguration dataSourceConfig)
        : base(settings, dataSourceConfig)
    {
        var hosts = settings.Host!.Split(',');
        _pools = new GaussDBDataSource[hosts.Length];
        for (var i = 0; i < hosts.Length; i++)
        {
            var poolSettings = settings.Clone();
            Debug.Assert(!poolSettings.Multiplexing);
            var host = hosts[i].AsSpan().Trim();
            if (GaussDBConnectionStringBuilder.TrySplitHostPort(host, out var newHost, out var newPort))
            {
                poolSettings.Host = newHost;
                poolSettings.Port = newPort;
            }
            else
                poolSettings.Host = host.ToString();

            _pools[i] = settings.Pooling
                ? new PoolingDataSource(poolSettings, dataSourceConfig)
                : new UnpooledDataSource(poolSettings, dataSourceConfig);
        }

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

    async ValueTask<GaussDBConnector?> TryGetIdleOrNew(
        GaussDBConnection conn,
        TimeSpan timeoutPerHost,
        bool async,
        TargetSessionAttributes preferredType, Func<DatabaseState, TargetSessionAttributes, bool> stateValidator,
        int poolIndex,
        IList<Exception> exceptions,
        CancellationToken cancellationToken)
    {
        var pools = _pools;
        for (var i = 0; i < pools.Length; i++)
        {
            var pool = pools[poolIndex];
            poolIndex++;
            if (poolIndex == pools.Length)
                poolIndex = 0;

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
        GaussDBConnection conn,
        TimeSpan timeoutPerHost,
        bool async,
        TargetSessionAttributes preferredType,
        Func<DatabaseState, TargetSessionAttributes, bool> stateValidator,
        int poolIndex,
        IList<Exception> exceptions,
        CancellationToken cancellationToken)
    {
        var pools = _pools;
        for (var i = 0; i < pools.Length; i++)
        {
            var pool = pools[poolIndex];
            poolIndex++;
            if (poolIndex == pools.Length)
                poolIndex = 0;

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

        var poolIndex = conn.Settings.LoadBalanceHosts ? GetRoundRobinIndex() : 0;

        var timeoutPerHost = timeout.IsSet ? timeout.CheckAndGetTimeLeft() : TimeSpan.Zero;
        var preferredType = GetTargetSessionAttributes(conn);
        var checkUnpreferred = preferredType is TargetSessionAttributes.PreferPrimary or TargetSessionAttributes.PreferStandby;

        var connector = await TryGetIdleOrNew(conn, timeoutPerHost, async, preferredType, IsPreferred, poolIndex, exceptions, cancellationToken).ConfigureAwait(false) ??
                        (checkUnpreferred ?
                            await TryGetIdleOrNew(conn, timeoutPerHost, async, preferredType, IsOnline, poolIndex, exceptions, cancellationToken).ConfigureAwait(false)
                            : null) ??
                        await TryGet(conn, timeoutPerHost, async, preferredType, IsPreferred, poolIndex, exceptions, cancellationToken).ConfigureAwait(false) ??
                        (checkUnpreferred ?
                            await TryGet(conn, timeoutPerHost, async, preferredType, IsOnline, poolIndex, exceptions, cancellationToken).ConfigureAwait(false)
                            : null);

        return connector ?? throw NoSuitableHostsException(exceptions);
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
        foreach (var pool in _pools)
            pool.Clear();
    }

    /// <summary>
    /// Clears the database state (primary, secondary, offline...) for all data sources managed by this multi-host data source.
    /// Can be useful to make GaussDB retry a PostgreSQL instance which was previously detected to be offline.
    /// </summary>
    public void ClearDatabaseStates()
    {
        foreach (var pool in _pools)
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

            foreach (var pool in _pools)
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
}
