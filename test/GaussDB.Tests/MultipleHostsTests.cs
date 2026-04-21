using HuaweiCloud.GaussDB.Internal;
using HuaweiCloud.GaussDB.Tests.Support;
using NUnit.Framework;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using HuaweiCloud.GaussDB.BackendMessages;
using HuaweiCloud.GaussDB.Properties;
using static HuaweiCloud.GaussDB.Tests.Support.MockState;
using static HuaweiCloud.GaussDB.Tests.TestUtil;
using IsolationLevel = System.Transactions.IsolationLevel;
using TransactionStatus = HuaweiCloud.GaussDB.Internal.TransactionStatus;

namespace HuaweiCloud.GaussDB.Tests;

#pragma warning disable CS0618

public class MultipleHostsTests : TestBase
{
    static readonly object[] MyCases =
    [
        new object[] { TargetSessionAttributes.Standby,        new[] { Primary,         Standby         }, 1 },
        new object[] { TargetSessionAttributes.Standby,        new[] { PrimaryReadOnly, Standby         }, 1 },
        new object[] { TargetSessionAttributes.PreferStandby,  new[] { Primary,         Standby         }, 1 },
        new object[] { TargetSessionAttributes.PreferStandby,  new[] { PrimaryReadOnly, Standby         }, 1 },
        new object[] { TargetSessionAttributes.PreferStandby,  new[] { Primary,         Primary         }, 0 },
        new object[] { TargetSessionAttributes.Primary,        new[] { Standby,         Primary         }, 1 },
        new object[] { TargetSessionAttributes.Primary,        new[] { Standby,         PrimaryReadOnly }, 1 },
        new object[] { TargetSessionAttributes.PreferPrimary,  new[] { Standby,         Primary         }, 1 },
        new object[] { TargetSessionAttributes.PreferPrimary,  new[] { Standby,         PrimaryReadOnly }, 1 },
        new object[] { TargetSessionAttributes.PreferPrimary,  new[] { Standby,         Standby         }, 0 },
        new object[] { TargetSessionAttributes.Any,            new[] { Standby,         Primary         }, 0 },
        new object[] { TargetSessionAttributes.Any,            new[] { Primary,         Standby         }, 0 },
        new object[] { TargetSessionAttributes.Any,            new[] { PrimaryReadOnly, Standby         }, 0 },
        new object[] { TargetSessionAttributes.ReadWrite,      new[] { Standby,         Primary         }, 1 },
        new object[] { TargetSessionAttributes.ReadWrite,      new[] { PrimaryReadOnly, Primary         }, 1 },
        new object[] { TargetSessionAttributes.ReadOnly,       new[] { Primary,         Standby         }, 1 },
        new object[] { TargetSessionAttributes.ReadOnly,       new[] { PrimaryReadOnly, Standby         }, 0 }
    ];

    [Test]
    [TestCaseSource(nameof(MyCases))]
    public async Task Connect_to_correct_host_pooled(TargetSessionAttributes targetSessionAttributes, MockState[] servers, int expectedServer)
    {
        var postmasters = servers.Select(s => PgPostmasterMock.Start(state: s)).ToArray();
        await using var __ = new DisposableWrapper(postmasters);

        var connectionStringBuilder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(postmasters),
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading,
            Pooling = true
        };

        await using var dataSource = new GaussDBDataSourceBuilder(connectionStringBuilder.ConnectionString)
            .BuildMultiHost();
        await using var conn = await dataSource.OpenConnectionAsync(targetSessionAttributes);

        Assert.That(conn.Port, Is.EqualTo(postmasters[expectedServer].Port));

        for (var i = 0; i <= expectedServer; i++)
            _ = await postmasters[i].WaitForServerConnection();
    }

    [Test]
    [TestCaseSource(nameof(MyCases))]
    public async Task Connect_to_correct_host_unpooled(TargetSessionAttributes targetSessionAttributes, MockState[] servers, int expectedServer)
    {
        var postmasters = servers.Select(s => PgPostmasterMock.Start(state: s)).ToArray();
        await using var __ = new DisposableWrapper(postmasters);

        var connectionStringBuilder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(postmasters),
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading,
            Pooling = false
        };

        await using var dataSource = new GaussDBDataSourceBuilder(connectionStringBuilder.ConnectionString)
            .BuildMultiHost();
        await using var conn = await dataSource.OpenConnectionAsync(targetSessionAttributes);

        Assert.That(conn.Port, Is.EqualTo(postmasters[expectedServer].Port));

        for (var i = 0; i <= expectedServer; i++)
            _ = await postmasters[i].WaitForServerConnection();
    }

    [Test]
    [TestCaseSource(nameof(MyCases))]
    public async Task Connect_to_correct_host_with_available_idle(
        TargetSessionAttributes targetSessionAttributes, MockState[] servers, int expectedServer)
    {
        var postmasters = servers.Select(s => PgPostmasterMock.Start(state: s)).ToArray();
        await using var __ = new DisposableWrapper(postmasters);

        // First, open and close a connection with the TargetSessionAttributes matching the first server.
        // This ensures wew have an idle connection in the pool.
        var connectionStringBuilder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(postmasters),
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading,
        };

        await using var dataSource = new GaussDBDataSourceBuilder(connectionStringBuilder.ConnectionString)
            .BuildMultiHost();
        var idleConnTargetSessionAttributes = servers[0] switch
        {
            Primary => TargetSessionAttributes.ReadWrite,
            PrimaryReadOnly => TargetSessionAttributes.ReadOnly,
            Standby => TargetSessionAttributes.Standby,
            _ => throw new ArgumentOutOfRangeException()
        };
        await using (_ = await dataSource.OpenConnectionAsync(idleConnTargetSessionAttributes))
        {
            // Do nothing, close to have an idle connection in the pool.
        }

        // Now connect with the test TargetSessionAttributes

        await using var conn = await dataSource.OpenConnectionAsync(targetSessionAttributes);

        Assert.That(conn.Port, Is.EqualTo(postmasters[expectedServer].Port));

        for (var i = 0; i <= expectedServer; i++)
            _ = await postmasters[i].WaitForServerConnection();
    }

    [Test]
    [TestCase(TargetSessionAttributes.Standby,   new[] { Primary,         Primary })]
    [TestCase(TargetSessionAttributes.Primary,   new[] { Standby,         Standby })]
    [TestCase(TargetSessionAttributes.ReadWrite, new[] { PrimaryReadOnly, Standby })]
    [TestCase(TargetSessionAttributes.ReadOnly,  new[] { Primary,         Primary })]
    public async Task Valid_host_not_found(TargetSessionAttributes targetSessionAttributes, MockState[] servers)
    {
        var postmasters = servers.Select(s => PgPostmasterMock.Start(state: s)).ToArray();
        await using var __ = new DisposableWrapper(postmasters);

        var connectionStringBuilder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(postmasters),
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading,
        };

        await using var dataSource = new GaussDBDataSourceBuilder(connectionStringBuilder.ConnectionString)
            .BuildMultiHost();

        var exception = Assert.ThrowsAsync<GaussDBException>(async () => await dataSource.OpenConnectionAsync(targetSessionAttributes))!;
        Assert.That(exception.Message, Is.EqualTo("No suitable host was found."));
        Assert.That(exception.InnerException, Is.Null);

        for (var i = 0; i < servers.Length; i++)
            _ = await postmasters[i].WaitForServerConnection();
    }

    [Test, Platform(Exclude = "MacOsX", Reason = "#3786")]
    public void All_hosts_are_down()
    {
        var endpoint = new IPEndPoint(IPAddress.Loopback, 0);

        using var socket1 = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        socket1.Bind(endpoint);
        var localEndPoint1 = (IPEndPoint)socket1.LocalEndPoint!;

        using var socket2 = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        socket2.Bind(endpoint);
        var localEndPoint2 = (IPEndPoint)socket2.LocalEndPoint!;

        // Note that we Bind (to reserve the port), but do not Listen - connection attempts will fail.

        var connectionString = new GaussDBConnectionStringBuilder
        {
            Host = $"{localEndPoint1.Address}:{localEndPoint1.Port},{localEndPoint2.Address}:{localEndPoint2.Port}"
        }.ConnectionString;
        using var dataSource = new GaussDBDataSourceBuilder(connectionString).BuildMultiHost();

        var exception = Assert.ThrowsAsync<GaussDBException>(async () => await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any))!;
        var aggregateException = (AggregateException)exception.InnerException!;
        Assert.That(aggregateException.InnerExceptions, Has.Count.EqualTo(2));

        for (var i = 0; i < aggregateException.InnerExceptions.Count; i++)
        {
            Assert.That(aggregateException.InnerExceptions[i], Is.TypeOf<GaussDBException>()
                .With.InnerException.TypeOf<SocketException>()
                .With.InnerException.Property(nameof(SocketException.SocketErrorCode)).EqualTo(SocketError.ConnectionRefused));
        }
    }

    [Test]
    public async Task All_hosts_are_unavailable(
        [Values] bool pooling,
        [Values(PostgresErrorCodes.InvalidCatalogName, PostgresErrorCodes.CannotConnectNow)] string errorCode)
    {
        await using var primaryPostmaster = PgPostmasterMock.Start(state: Primary, startupErrorCode: errorCode);
        await using var standbyPostmaster = PgPostmasterMock.Start(state: Standby, startupErrorCode: errorCode);

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(primaryPostmaster, standbyPostmaster),
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading,
            Pooling = pooling,
        };

        await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).BuildMultiHost();

        var ex = Assert.ThrowsAsync<PostgresException>(async () => await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any))!;
        Assert.That(ex.SqlState, Is.EqualTo(errorCode));
    }

    [Test]
    [Platform(Exclude = "MacOsX", Reason = "Flaky in CI on Mac")]
    public async Task First_host_is_down()
    {
        using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        var endpoint = new IPEndPoint(IPAddress.Loopback, 0);
        socket.Bind(endpoint);
        var localEndPoint = (IPEndPoint)socket.LocalEndPoint!;
        // Note that we Bind (to reserve the port), but do not Listen - connection attempts will fail.

        await using var postmaster = PgPostmasterMock.Start(state: Primary);

        var connectionString = new GaussDBConnectionStringBuilder
        {
            Host = $"{localEndPoint.Address}:{localEndPoint.Port},{postmaster.Host}:{postmaster.Port}",
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        }.ConnectionString;

        await using var dataSource = new GaussDBDataSourceBuilder(connectionString).BuildMultiHost();

        await using var conn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);
        Assert.That(conn.Port, Is.EqualTo(postmaster.Port));
    }

    [Test]
    [TestCase("any")]
    [TestCase("primary")]
    [TestCase("standby")]
    [TestCase("prefer-primary")]
    [TestCase("prefer-standby")]
    [TestCase("read-write")]
    [TestCase("read-only")]
    public async Task TargetSessionAttributes_with_single_host(string targetSessionAttributes)
    {
        var connectionString = new GaussDBConnectionStringBuilder(ConnectionString)
        {
            TargetSessionAttributes = targetSessionAttributes
        }.ConnectionString;

        if (targetSessionAttributes == "any")
        {
            await using var postmasterMock = PgPostmasterMock.Start(ConnectionString);
            using var pool = CreateTempPool(postmasterMock.ConnectionString, out connectionString);
            await using var conn = new GaussDBConnection(connectionString);
            await conn.OpenAsync();
            _ = await postmasterMock.WaitForServerConnection();
        }
        else
        {
            Assert.That(() => new GaussDBConnection(connectionString), Throws.Exception.TypeOf<NotSupportedException>());
        }
    }

    [Test]
    public void TargetSessionAttributes_default_is_null()
        => Assert.That(new GaussDBConnectionStringBuilder().TargetSessionAttributes, Is.Null);

    [Test]
    [NonParallelizable] // Sets environment variable
    public async Task TargetSessionAttributes_uses_environment_variable()
    {
        using var envVarResetter = SetEnvironmentVariable("PGTARGETSESSIONATTRS", "prefer-standby");

        await using var primaryPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var standbyPostmaster = PgPostmasterMock.Start(state: Standby);

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(primaryPostmaster, standbyPostmaster),
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        };

        Assert.That(builder.TargetSessionAttributes, Is.Null);

        await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString)
            .BuildMultiHost();

        await using var conn = await dataSource.OpenConnectionAsync();
        Assert.That(conn.Port, Is.EqualTo(standbyPostmaster.Port));
    }

    [Test]
    public void TargetSessionAttributes_invalid_throws()
        => Assert.Throws<ArgumentException>(() =>
            new GaussDBConnectionStringBuilder
            {
                TargetSessionAttributes = nameof(TargetSessionAttributes_invalid_throws)
            });

    [Test]
    public void HostRecheckSeconds_default_value()
    {
        var builder = new GaussDBConnectionStringBuilder();
        Assert.That(builder.HostRecheckSeconds, Is.EqualTo(10));
        Assert.That(builder.HostRecheckSecondsTranslated, Is.EqualTo(TimeSpan.FromSeconds(10)));
    }

    [Test]
    public void HostRecheckSeconds_zero_value()
    {
        var builder = new GaussDBConnectionStringBuilder
        {
            HostRecheckSeconds = 0,
        };
        Assert.That(builder.HostRecheckSeconds, Is.EqualTo(0));
        Assert.That(builder.HostRecheckSecondsTranslated, Is.EqualTo(TimeSpan.FromSeconds(-1)));
    }

    [Test]
    public void HostRecheckSeconds_invalid_throws()
        => Assert.Throws<ArgumentOutOfRangeException>(() =>
            new GaussDBConnectionStringBuilder
            {
                HostRecheckSeconds = -1
            });

    [Test]
    public async Task Connect_with_load_balancing()
    {
        await using var primaryPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var standbyPostmaster = PgPostmasterMock.Start(state: Standby);

        var defaultCsb = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(primaryPostmaster, standbyPostmaster),
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading,
            MaxPoolSize = 1,
            LoadBalanceHosts = true,
        };

        await using var dataSource = new GaussDBDataSourceBuilder(defaultCsb.ConnectionString)
            .BuildMultiHost();

        GaussDBConnector firstConnector;
        GaussDBConnector secondConnector;

        await using (var firstConnection = await dataSource.OpenConnectionAsync())
        {
            firstConnector = firstConnection.Connector!;
        }

        await using (var secondConnection = await dataSource.OpenConnectionAsync())
        {
            secondConnector = secondConnection.Connector!;
        }

        Assert.AreNotSame(firstConnector, secondConnector);

        await using (var firstBalancedConnection = await dataSource.OpenConnectionAsync())
        {
            Assert.AreSame(firstConnector, firstBalancedConnection.Connector);
        }

        await using (var secondBalancedConnection = await dataSource.OpenConnectionAsync())
        {
            Assert.AreSame(secondConnector, secondBalancedConnection.Connector);
        }

        await using (var thirdBalancedConnection = await dataSource.OpenConnectionAsync())
        {
            Assert.AreSame(firstConnector, thirdBalancedConnection.Connector);
        }
    }

    [Test]
    public async Task Connect_without_load_balancing()
    {
        await using var primaryPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var standbyPostmaster = PgPostmasterMock.Start(state: Standby);

        var defaultCsb = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(primaryPostmaster, standbyPostmaster),
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading,
            MaxPoolSize = 1,
            LoadBalanceHosts = false,
        };

        await using var dataSource = new GaussDBDataSourceBuilder(defaultCsb.ConnectionString)
            .BuildMultiHost();

        GaussDBConnector firstConnector;
        GaussDBConnector secondConnector;

        await using (var firstConnection = await dataSource.OpenConnectionAsync())
        {
            firstConnector = firstConnection.Connector!;
        }
        await using (var secondConnection = await dataSource.OpenConnectionAsync())
        {
            Assert.AreSame(firstConnector, secondConnection.Connector);
        }
        await using (var firstConnection = await dataSource.OpenConnectionAsync())
        await using (var secondConnection = await dataSource.OpenConnectionAsync())
        {
            secondConnector = secondConnection.Connector!;
        }

        Assert.AreNotSame(firstConnector, secondConnector);

        await using (var firstUnbalancedConnection = await dataSource.OpenConnectionAsync())
        {
            Assert.AreSame(firstConnector, firstUnbalancedConnection.Connector);
        }

        await using (var secondUnbalancedConnection = await dataSource.OpenConnectionAsync())
        {
            Assert.AreSame(firstConnector, secondUnbalancedConnection.Connector);
        }
    }

    [Test]
    public async Task Connect_state_changing_hosts([Values] bool alwaysCheckHostState)
    {
        await using var primaryPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var standbyPostmaster = PgPostmasterMock.Start(state: Standby);

        var defaultCsb = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(primaryPostmaster, standbyPostmaster),
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading,
            MaxPoolSize = 1,
            HostRecheckSeconds = alwaysCheckHostState ? 0 : int.MaxValue,
            NoResetOnClose = true,
        };

        await using var dataSource = new GaussDBDataSourceBuilder(defaultCsb.ConnectionString)
            .BuildMultiHost();

        GaussDBConnector firstConnector;
        GaussDBConnector secondConnector;
        var firstServerTask = Task.Run(async () =>
        {
            var server = await primaryPostmaster.WaitForServerConnection();
            if (!alwaysCheckHostState)
                return;

            // If we always check the host, we will send the request for the state
            // even though we got one while opening the connection
            await server.SendMockState(Primary);

            // Update the state after a 'failover'
            await server.SendMockState(Standby);
        });
        var secondServerTask = Task.Run(async () =>
        {
            var server = await standbyPostmaster.WaitForServerConnection();
            if (!alwaysCheckHostState)
                return;

            // If we always check the host, we will send the request for the state
            // even though we got one while opening the connection
            await server.SendMockState(Standby);

            // As TargetSessionAttributes is 'prefer', it does another cycle for the 'unpreferred'
            await server.SendMockState(Standby);
            // Update the state after a 'failover'
            await server.SendMockState(Primary);
        });

        await using (var firstConnection = await dataSource.OpenConnectionAsync(TargetSessionAttributes.PreferPrimary))
        await using (var secondConnection = await dataSource.OpenConnectionAsync(TargetSessionAttributes.PreferPrimary))
        {
            firstConnector = firstConnection.Connector!;
            secondConnector = secondConnection.Connector!;
        }

        await using var thirdConnection = await dataSource.OpenConnectionAsync(TargetSessionAttributes.PreferPrimary);
        Assert.AreSame(alwaysCheckHostState ? secondConnector : firstConnector, thirdConnection.Connector);

        await firstServerTask;
        await secondServerTask;
    }

    [Test]
    public void Database_state_cache_basic()
    {
        using var dataSource = CreateDataSource();
        var timeStamp = DateTime.UtcNow;

        dataSource.UpdateDatabaseState(DatabaseState.PrimaryReadWrite, timeStamp, TimeSpan.Zero);
        Assert.AreEqual(DatabaseState.PrimaryReadWrite, dataSource.GetDatabaseState());

        // Update with the same timestamp - shouldn't change anything
        dataSource.UpdateDatabaseState(DatabaseState.Standby, timeStamp, TimeSpan.Zero);
        Assert.AreEqual(DatabaseState.PrimaryReadWrite, dataSource.GetDatabaseState());

        // Update with a new timestamp
        timeStamp = timeStamp.AddSeconds(1);
        dataSource.UpdateDatabaseState(DatabaseState.PrimaryReadOnly, timeStamp, TimeSpan.Zero);
        Assert.AreEqual(DatabaseState.PrimaryReadOnly, dataSource.GetDatabaseState());

        // Expired state returns as Unknown (depending on ignoreExpiration)
        timeStamp = timeStamp.AddSeconds(1);
        dataSource.UpdateDatabaseState(DatabaseState.PrimaryReadWrite, timeStamp, TimeSpan.FromSeconds(-1));
        Assert.AreEqual(DatabaseState.Unknown, dataSource.GetDatabaseState(ignoreExpiration: false));
        Assert.AreEqual(DatabaseState.PrimaryReadWrite, dataSource.GetDatabaseState(ignoreExpiration: true));
    }

    [Test]
    public async Task Offline_state_on_connection_failure()
    {
        await using var server = PgPostmasterMock.Start(ConnectionString, startupErrorCode: PostgresErrorCodes.ConnectionFailure);
        await using var dataSource = server.CreateDataSource();
        await using var conn = dataSource.CreateConnection();

        var ex = Assert.ThrowsAsync<PostgresException>(conn.OpenAsync)!;
        Assert.That(ex.SqlState, Is.EqualTo(PostgresErrorCodes.ConnectionFailure));

        var state = conn.GaussDBDataSource.GetDatabaseState();
        Assert.That(state, Is.EqualTo(DatabaseState.Offline));
    }

    [Test]
    public async Task Unknown_state_on_connection_authentication_failure()
    {
        await using var server = PgPostmasterMock.Start(ConnectionString, startupErrorCode: PostgresErrorCodes.InvalidAuthorizationSpecification);
        await using var dataSource = server.CreateDataSource();
        await using var conn = dataSource.CreateConnection();

        var ex = Assert.ThrowsAsync<PostgresException>(conn.OpenAsync)!;
        Assert.That(ex.SqlState, Is.EqualTo(PostgresErrorCodes.InvalidAuthorizationSpecification));

        var state = conn.GaussDBDataSource.GetDatabaseState();
        Assert.That(state, Is.EqualTo(DatabaseState.Unknown));
    }

    [Test]
    public async Task Offline_state_on_query_execution_pg_critical_failure()
    {
        await using var postmaster = PgPostmasterMock.Start(ConnectionString);
        await using var dataSource = postmaster.CreateDataSource();
        await using var conn = await dataSource.OpenConnectionAsync();
        await using var anotherConn = await dataSource.OpenConnectionAsync();
        await anotherConn.CloseAsync();

        var state = conn.GaussDBDataSource.GetDatabaseState();
        Assert.That(state, Is.EqualTo(DatabaseState.Unknown));
        Assert.That(conn.GaussDBDataSource.Statistics.Total, Is.EqualTo(2));

        var server = await postmaster.WaitForServerConnection();
        await server.WriteErrorResponse(PostgresErrorCodes.CrashShutdown).FlushAsync();

        var ex = Assert.ThrowsAsync<PostgresException>(() => conn.ExecuteNonQueryAsync("SELECT 1"))!;
        Assert.That(ex.SqlState, Is.EqualTo(PostgresErrorCodes.CrashShutdown));
        Assert.That(conn.State, Is.EqualTo(ConnectionState.Closed));

        state = conn.GaussDBDataSource.GetDatabaseState();
        Assert.That(state, Is.EqualTo(DatabaseState.Offline));
        Assert.That(conn.GaussDBDataSource.Statistics.Total, Is.EqualTo(0));
    }

    [Test, NonParallelizable]
    public async Task Offline_state_on_query_execution_pg_non_critical_failure()
    {
        await using var dataSource = CreateDataSource();
        await using var conn = await dataSource.OpenConnectionAsync();

        // Starting with PG14 we get the cluster's state from PG automatically
        var expectedState = conn.PostgreSqlVersion.Major > 13 ? DatabaseState.PrimaryReadWrite : DatabaseState.Unknown;

        var state = dataSource.GetDatabaseState();
        Assert.That(state, Is.EqualTo(expectedState));
        Assert.That(dataSource.Statistics.Total, Is.EqualTo(1));

        var ex = Assert.ThrowsAsync<PostgresException>(() => conn.ExecuteNonQueryAsync("SELECT abc"))!;
        Assert.That(ex.SqlState, Is.EqualTo(PostgresErrorCodes.UndefinedColumn));
        Assert.That(conn.State, Is.EqualTo(ConnectionState.Open));

        state = dataSource.GetDatabaseState();
        Assert.That(state, Is.EqualTo(expectedState));
        Assert.That(dataSource.Statistics.Total, Is.EqualTo(1));
    }

    [Test]
    public async Task Offline_state_on_query_execution_IOException()
    {
        await using var postmaster = PgPostmasterMock.Start(ConnectionString);
        await using var dataSource = postmaster.CreateDataSource();
        await using var conn = await dataSource.OpenConnectionAsync();
        await using var anotherConn = await dataSource.OpenConnectionAsync();
        await anotherConn.CloseAsync();

        var state = conn.GaussDBDataSource.GetDatabaseState();
        Assert.That(state, Is.EqualTo(DatabaseState.Unknown));
        Assert.That(conn.GaussDBDataSource.Statistics.Total, Is.EqualTo(2));

        var server = await postmaster.WaitForServerConnection();
        server.Close();

        var ex = Assert.ThrowsAsync<GaussDBException>(() => conn.ExecuteNonQueryAsync("SELECT 1"))!;
        Assert.That(ex.InnerException, Is.InstanceOf<IOException>());
        Assert.That(conn.State, Is.EqualTo(ConnectionState.Closed));

        state = conn.GaussDBDataSource.GetDatabaseState();
        Assert.That(state, Is.EqualTo(DatabaseState.Offline));
        Assert.That(conn.GaussDBDataSource.Statistics.Total, Is.EqualTo(0));
    }

    [Test]
    public async Task Offline_state_on_query_execution_TimeoutException()
    {
        await using var postmaster = PgPostmasterMock.Start(ConnectionString);
        await using var dataSource = postmaster.CreateDataSource(builder =>
        {
            builder.ConnectionStringBuilder.CommandTimeout = 1;
            builder.ConnectionStringBuilder.CancellationTimeout = 1;
        });

        await using var conn = await dataSource.OpenConnectionAsync();
        await using var anotherConn = await dataSource.OpenConnectionAsync();
        await anotherConn.CloseAsync();

        var state = conn.GaussDBDataSource.GetDatabaseState();
        Assert.That(state, Is.EqualTo(DatabaseState.Unknown));
        Assert.That(conn.GaussDBDataSource.Statistics.Total, Is.EqualTo(2));

        var ex = Assert.ThrowsAsync<GaussDBException>(() => conn.ExecuteNonQueryAsync("SELECT 1"))!;
        Assert.That(ex.InnerException, Is.TypeOf<TimeoutException>());
        Assert.That(conn.State, Is.EqualTo(ConnectionState.Closed));

        state = conn.GaussDBDataSource.GetDatabaseState();
        Assert.That(state, Is.EqualTo(DatabaseState.Offline));
        Assert.That(conn.GaussDBDataSource.Statistics.Total, Is.EqualTo(0));
    }

    [Test]
    public async Task Unknown_state_on_query_execution_TimeoutException_with_disabled_cancellation()
    {
        await using var postmaster = PgPostmasterMock.Start(ConnectionString);
        await using var dataSource = postmaster.CreateDataSource(builder =>
        {
            builder.ConnectionStringBuilder.CommandTimeout = 1;
            builder.ConnectionStringBuilder.CancellationTimeout = -1;
        });

        await using var conn = await dataSource.OpenConnectionAsync();
        await using var anotherConn = await dataSource.OpenConnectionAsync();
        await anotherConn.CloseAsync();

        var state = conn.GaussDBDataSource.GetDatabaseState();
        Assert.That(state, Is.EqualTo(DatabaseState.Unknown));
        Assert.That(conn.GaussDBDataSource.Statistics.Total, Is.EqualTo(2));

        var ex = Assert.ThrowsAsync<GaussDBException>(() => conn.ExecuteNonQueryAsync("SELECT 1"))!;
        Assert.That(ex.InnerException, Is.TypeOf<TimeoutException>());
        Assert.That(conn.State, Is.EqualTo(ConnectionState.Closed));

        state = conn.GaussDBDataSource.GetDatabaseState();
        Assert.That(state, Is.EqualTo(DatabaseState.Unknown));
        Assert.That(conn.GaussDBDataSource.Statistics.Total, Is.EqualTo(1));
    }

    [Test]
    public async Task Unknown_state_on_query_execution_cancellation_with_disabled_cancellation_timeout()
    {
        await using var postmaster = PgPostmasterMock.Start(ConnectionString);
        await using var dataSource = postmaster.CreateDataSource(builder =>
        {
            builder.ConnectionStringBuilder.CommandTimeout = 30;
            builder.ConnectionStringBuilder.CancellationTimeout = -1;
        });

        await using var conn = await dataSource.OpenConnectionAsync();
        await using var anotherConn = await dataSource.OpenConnectionAsync();
        await anotherConn.CloseAsync();

        var state = conn.GaussDBDataSource.GetDatabaseState();
        Assert.That(state, Is.EqualTo(DatabaseState.Unknown));
        Assert.That(conn.GaussDBDataSource.Statistics.Total, Is.EqualTo(2));

        using var cts = new CancellationTokenSource();

        var query = conn.ExecuteNonQueryAsync("SELECT 1", cancellationToken: cts.Token);
        cts.Cancel();
        var ex = Assert.ThrowsAsync<OperationCanceledException>(async () => await query)!;
        Assert.That(ex.InnerException, Is.TypeOf<TimeoutException>());
        Assert.That(conn.State, Is.EqualTo(ConnectionState.Closed));

        state = conn.GaussDBDataSource.GetDatabaseState();
        Assert.That(state, Is.EqualTo(DatabaseState.Unknown));
        Assert.That(conn.GaussDBDataSource.Statistics.Total, Is.EqualTo(1));
    }

    [Test]
    public async Task Unknown_state_on_query_execution_TimeoutException_with_cancellation_failure()
    {
        await using var postmaster = PgPostmasterMock.Start(ConnectionString);
        await using var dataSource = postmaster.CreateDataSource(builder =>
        {
            builder.ConnectionStringBuilder.CommandTimeout = 1;
            builder.ConnectionStringBuilder.CancellationTimeout = 0;
        });

        await using var conn = await dataSource.OpenConnectionAsync();

        var state = conn.GaussDBDataSource.GetDatabaseState();
        Assert.That(state, Is.EqualTo(DatabaseState.Unknown));
        Assert.That(conn.GaussDBDataSource.Statistics.Total, Is.EqualTo(1));

        var server = await postmaster.WaitForServerConnection();

        var query = conn.ExecuteNonQueryAsync("SELECT 1");

        await postmaster.WaitForCancellationRequest();
        await server.WriteCancellationResponse().WriteReadyForQuery().FlushAsync();

        var ex = Assert.ThrowsAsync<GaussDBException>(async () => await query)!;
        Assert.That(ex.InnerException, Is.TypeOf<TimeoutException>());
        Assert.That(conn.State, Is.EqualTo(ConnectionState.Open));

        state = conn.GaussDBDataSource.GetDatabaseState();
        Assert.That(state, Is.EqualTo(DatabaseState.Unknown));
        Assert.That(conn.GaussDBDataSource.Statistics.Total, Is.EqualTo(1));
    }

    [Test]
    public async Task Clear_pool_one_host_only_on_admin_shutdown()
    {
        await using var primaryPostmaster = PgPostmasterMock.Start(ConnectionString, state: Primary);
        await using var standbyPostmaster = PgPostmasterMock.Start(ConnectionString, state: Standby);
        var dataSourceBuilder = new GaussDBDataSourceBuilder
        {
            ConnectionStringBuilder =
            {
                Host = MultipleHosts(primaryPostmaster, standbyPostmaster),
                ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading,
                MaxPoolSize = 2
            }
        };
        await using var multiHostDataSource = dataSourceBuilder.BuildMultiHost();
        await using var preferPrimaryDataSource = multiHostDataSource.WithTargetSession(TargetSessionAttributes.PreferPrimary);

        await using var primaryConn = await preferPrimaryDataSource.OpenConnectionAsync();
        await using var anotherPrimaryConn = await preferPrimaryDataSource.OpenConnectionAsync();
        await using var standbyConn = await preferPrimaryDataSource.OpenConnectionAsync();
        var primaryDataSource = primaryConn.Connector!.DataSource;
        var standbyDataSource = standbyConn.Connector!.DataSource;
        await anotherPrimaryConn.CloseAsync();
        await standbyConn.CloseAsync();

        Assert.That(primaryDataSource.GetDatabaseState(), Is.EqualTo(DatabaseState.PrimaryReadWrite));
        Assert.That(standbyDataSource.GetDatabaseState(), Is.EqualTo(DatabaseState.Standby));
        Assert.That(primaryConn.GaussDBDataSource.Statistics.Total, Is.EqualTo(3));

        var server = await primaryPostmaster.WaitForServerConnection();
        await server.WriteErrorResponse(PostgresErrorCodes.AdminShutdown).FlushAsync();

        var ex = Assert.ThrowsAsync<PostgresException>(() => primaryConn.ExecuteNonQueryAsync("SELECT 1"))!;
        Assert.That(ex.SqlState, Is.EqualTo(PostgresErrorCodes.AdminShutdown));
        Assert.That(primaryConn.State, Is.EqualTo(ConnectionState.Closed));

        Assert.That(primaryDataSource.GetDatabaseState(), Is.EqualTo(DatabaseState.Offline));
        Assert.That(standbyDataSource.GetDatabaseState(), Is.EqualTo(DatabaseState.Standby));
        Assert.That(primaryConn.GaussDBDataSource.Statistics.Total, Is.EqualTo(1));

        multiHostDataSource.ClearDatabaseStates();
        Assert.That(primaryDataSource.GetDatabaseState(), Is.EqualTo(DatabaseState.Unknown));
        Assert.That(standbyDataSource.GetDatabaseState(), Is.EqualTo(DatabaseState.Unknown));
    }

    [Test]
    [TestCase("any", true)]
    [TestCase("primary", true)]
    [TestCase("standby", false)]
    [TestCase("prefer-primary", true)]
    [TestCase("prefer-standby", false)]
    [TestCase("read-write", true)]
    [TestCase("read-only", false)]
    public async Task Transaction_enlist_reuses_connection(string targetSessionAttributes, bool primary)
    {
        await using var primaryPostmaster = PgPostmasterMock.Start(ConnectionString, state: Primary);
        await using var standbyPostmaster = PgPostmasterMock.Start(ConnectionString, state: Standby);
        var csb = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(primaryPostmaster, standbyPostmaster),
            TargetSessionAttributes = targetSessionAttributes,
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading,
            MaxPoolSize = 10,
        };

        using var _ = CreateTempPool(csb, out var connString);

        using var scope = new TransactionScope(TransactionScopeOption.Required,
            new TransactionOptions { IsolationLevel = IsolationLevel.ReadCommitted }, TransactionScopeAsyncFlowOption.Enabled);

        var query1Task = Query(connString);

        var server = primary
            ? await primaryPostmaster.WaitForServerConnection()
            : await standbyPostmaster.WaitForServerConnection();

        await server
            .WriteCommandComplete()
            .WriteReadyForQuery(TransactionStatus.InTransactionBlock)
            .WriteParseComplete()
            .WriteBindComplete()
            .WriteNoData()
            .WriteCommandComplete()
            .WriteReadyForQuery(TransactionStatus.InTransactionBlock)
            .FlushAsync();
        await query1Task;

        var query2Task = Query(connString);
        await server
            .WriteParseComplete()
            .WriteBindComplete()
            .WriteNoData()
            .WriteCommandComplete()
            .WriteReadyForQuery(TransactionStatus.InTransactionBlock)
            .FlushAsync();
        await query2Task;

        await server
            .WriteCommandComplete()
            .WriteReadyForQuery()
            .FlushAsync();
        scope.Complete();

        async Task Query(string connectionString)
        {
            await using var conn = new GaussDBConnection(connectionString);
            await conn.OpenAsync();

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT 1";
            await cmd.ExecuteNonQueryAsync();
        }
    }

    [Test]
    public async Task Primary_host_failover_can_connect()
    {
        await using var firstPostmaster = PgPostmasterMock.Start(ConnectionString, state: Primary);
        await using var secondPostmaster = PgPostmasterMock.Start(ConnectionString, state: Standby);
        var dataSourceBuilder = new GaussDBDataSourceBuilder
        {
            ConnectionStringBuilder =
            {
                Host = MultipleHosts(firstPostmaster, secondPostmaster),
                ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading,
                HostRecheckSeconds = 5
            }
        };
        await using var multiHostDataSource = dataSourceBuilder.BuildMultiHost();
        var (firstDataSource, secondDataSource) = (multiHostDataSource.Pools[0], multiHostDataSource.Pools[1]);
        await using var primaryDataSource = multiHostDataSource.WithTargetSession(TargetSessionAttributes.Primary);

        await using var conn = await primaryDataSource.OpenConnectionAsync();
        Assert.That(conn.Port, Is.EqualTo(firstPostmaster.Port));
        var firstServer = await firstPostmaster.WaitForServerConnection();
        await firstServer
            .WriteErrorResponse(PostgresErrorCodes.AdminShutdown)
            .FlushAsync();

        var failoverEx = Assert.ThrowsAsync<PostgresException>(async () => await conn.ExecuteNonQueryAsync("SELECT 1"))!;
        Assert.That(failoverEx.SqlState, Is.EqualTo(PostgresErrorCodes.AdminShutdown));

        var noHostFoundEx = Assert.ThrowsAsync<GaussDBException>(async () => await conn.OpenAsync())!;
        Assert.That(noHostFoundEx.Message, Is.EqualTo("No suitable host was found."));

        Assert.That(firstDataSource.GetDatabaseState(), Is.EqualTo(DatabaseState.Offline));
        Assert.That(secondDataSource.GetDatabaseState(), Is.EqualTo(DatabaseState.Standby));

        firstPostmaster.State = Standby;
        secondPostmaster.State = Primary;
        var secondServer = await secondPostmaster.WaitForServerConnection();
        await secondServer.SendMockState(Primary);

        await Task.Delay(TimeSpan.FromSeconds(10));
        Assert.That(firstDataSource.GetDatabaseState(), Is.EqualTo(DatabaseState.Unknown));
        Assert.That(secondDataSource.GetDatabaseState(), Is.EqualTo(DatabaseState.Unknown));

        await conn.OpenAsync();
        Assert.That(conn.Port, Is.EqualTo(secondPostmaster.Port));
        Assert.That(firstDataSource.GetDatabaseState(), Is.EqualTo(DatabaseState.Standby));
        Assert.That(secondDataSource.GetDatabaseState(), Is.EqualTo(DatabaseState.PrimaryReadWrite));
    }

    [Test]
    public async Task AutoReconnect_retries_eligible_failover_error()
    {
        await using var firstPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var secondPostmaster = PgPostmasterMock.Start(state: Standby);

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(firstPostmaster, secondPostmaster),
            AutoReconnect = true,
            MaxReconnects = 2,
            Pooling = false,
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        };

        await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).BuildMultiHost();
        await using var primaryDataSource = dataSource.WithTargetSession(TargetSessionAttributes.Primary);
        await using var conn = await primaryDataSource.OpenConnectionAsync();

        Assert.That(conn.Port, Is.EqualTo(firstPostmaster.Port));

        var firstServer = await firstPostmaster.WaitForServerConnection();

        firstPostmaster.State = Standby;
        secondPostmaster.State = Primary;

        var queryTask = conn.ExecuteScalarAsync("SELECT 1");

        await firstServer.ExpectExtendedQuery();
        await firstServer
            .WriteErrorResponse(PostgresErrorCodes.AdminShutdown)
            .WriteReadyForQuery()
            .FlushAsync();

        var secondServer = await secondPostmaster.WaitForServerConnection();
        await secondServer.ExpectExtendedQuery();
        await secondServer.WriteScalarResponseAndFlush(1);

        Assert.That(await queryTask, Is.EqualTo(1));
        Assert.That(conn.Port, Is.EqualTo(secondPostmaster.Port));
    }

    [Test]
    public async Task AutoReconnect_exhaustion_leaves_connection_closed()
    {
        var postmaster = PgPostmasterMock.Start(state: Primary);
        var postmasterDisposed = false;

        try
        {
            var builder = new GaussDBConnectionStringBuilder
            {
                Host = $"{postmaster.Host}:{postmaster.Port}",
                AutoReconnect = true,
                MaxReconnects = 2,
                Pooling = false,
                ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
            };

            await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).BuildMultiHost();
            await using var conn = await dataSource.OpenConnectionAsync();

            _ = await postmaster.WaitForServerConnection();
            await postmaster.DisposeAsync();
            postmasterDisposed = true;

            Assert.ThrowsAsync<GaussDBException>(async () => await conn.ExecuteNonQueryAsync("SELECT 1"));
            Assert.That(conn.State, Is.EqualTo(ConnectionState.Closed));
        }
        finally
        {
            if (!postmasterDisposed)
                await postmaster.DisposeAsync();
        }
    }

    [Test]
    public async Task AutoReconnect_does_not_replay_explicit_transaction()
    {
        await using var firstPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var secondPostmaster = PgPostmasterMock.Start(state: Standby);

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(firstPostmaster, secondPostmaster),
            AutoReconnect = true,
            MaxReconnects = 2,
            Pooling = false,
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        };

        await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).BuildMultiHost();
        await using var primaryDataSource = dataSource.WithTargetSession(TargetSessionAttributes.Primary);
        await using var conn = await primaryDataSource.OpenConnectionAsync();
        var firstServer = await firstPostmaster.WaitForServerConnection();
        await using var tx = await conn.BeginTransactionAsync();

        firstPostmaster.State = Standby;
        secondPostmaster.State = Primary;

        var reconnectProbe = secondPostmaster.WaitForServerConnection().AsTask();
        var queryTask = conn.ExecuteNonQueryAsync("SELECT 1");

        await firstServer.ExpectSimpleQuery("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED");
        await firstServer
            .WriteCommandComplete()
            .WriteReadyForQuery(TransactionStatus.InTransactionBlock)
            .FlushAsync();
        await firstServer.ExpectExtendedQuery();
        await firstServer
            .WriteErrorResponse(PostgresErrorCodes.AdminShutdown)
            .WriteReadyForQuery(TransactionStatus.InFailedTransactionBlock)
            .FlushAsync();

        var exception = Assert.ThrowsAsync<PostgresException>(async () => await queryTask)!;
        Assert.That(exception.SqlState, Is.EqualTo(PostgresErrorCodes.AdminShutdown));
        Assert.That(exception.IsTransient, Is.True);
        await Task.Delay(200);
        Assert.That(reconnectProbe.IsCompleted, Is.False);
    }

    [Test]
    public async Task AutoReconnect_does_not_replay_active_reader()
    {
        await using var firstPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var secondPostmaster = PgPostmasterMock.Start(state: Standby);

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(firstPostmaster, secondPostmaster),
            AutoReconnect = true,
            MaxReconnects = 2,
            Pooling = false,
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        };

        await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).BuildMultiHost();
        await using var primaryDataSource = dataSource.WithTargetSession(TargetSessionAttributes.Primary);
        await using var conn = await primaryDataSource.OpenConnectionAsync();
        var firstServer = await firstPostmaster.WaitForServerConnection();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1";

        var readerTask = cmd.ExecuteReaderAsync();
        await firstServer.ExpectExtendedQuery();
        await firstServer
            .WriteParseComplete()
            .WriteBindComplete()
            .WriteRowDescription(new FieldDescription(23))
            .WriteDataRow(BitConverter.GetBytes(BinaryPrimitives.ReverseEndianness(1)))
            .FlushAsync();

        await using var reader = await readerTask;
        Assert.That(await reader.ReadAsync(), Is.True);
        Assert.That(reader.GetInt32(0), Is.EqualTo(1));

        firstPostmaster.State = Standby;
        secondPostmaster.State = Primary;

        var reconnectProbe = secondPostmaster.WaitForServerConnection().AsTask();
        firstServer.Close();

        Assert.ThrowsAsync<GaussDBException>(async () => await reader.ReadAsync());
        await Task.Delay(200);
        Assert.That(reconnectProbe.IsCompleted, Is.False);
    }

    [Test]
    [NonParallelizable]
    public async Task PriorityServers_proxy_primary_unreachable_falls_back_to_secondary_seed_cluster()
    {
        GaussDBGlobalClusterStatusTracker.Reset();
        GaussDBCoordinatorListTracker.Reset();

        await using var firstPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var secondPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var firstProxy = TcpFaultProxy.Start(firstPostmaster.Host, firstPostmaster.Port);
        await using var secondProxy = TcpFaultProxy.Start(secondPostmaster.Host, secondPostmaster.Port);

        await firstProxy.DisableAsync();

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(firstProxy, secondProxy),
            PriorityServers = 1,
            Pooling = false,
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        };

        await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).BuildMultiHost();
        await using var connection = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);

        Assert.That(connection.Port, Is.EqualTo(secondProxy.Port));
        _ = await secondPostmaster.WaitForServerConnection();
    }

    [Test]
    [NonParallelizable]
    public async Task AutoReconnect_proxy_disconnect_does_not_replay_current_command()
    {
        await using var firstPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var secondPostmaster = PgPostmasterMock.Start(state: Standby);
        await using var firstProxy = TcpFaultProxy.Start(firstPostmaster.Host, firstPostmaster.Port);
        await using var secondProxy = TcpFaultProxy.Start(secondPostmaster.Host, secondPostmaster.Port);

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(firstProxy, secondProxy),
            AutoReconnect = true,
            MaxReconnects = 2,
            Pooling = false,
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        };

        await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).BuildMultiHost();
        await using var primaryDataSource = dataSource.WithTargetSession(TargetSessionAttributes.Primary);
        await using var conn = await primaryDataSource.OpenConnectionAsync();

        Assert.That(conn.Port, Is.EqualTo(firstProxy.Port));
        _ = await firstPostmaster.WaitForServerConnection();

        firstPostmaster.State = Standby;
        secondPostmaster.State = Primary;
        await firstProxy.DisableAsync();
        var reconnectProbe = secondPostmaster.WaitForServerConnection().AsTask();

        Assert.ThrowsAsync<GaussDBException>(async () => await conn.ExecuteScalarAsync("SELECT 1"));
        Assert.That(conn.State, Is.EqualTo(ConnectionState.Closed));
        await Task.Delay(200);
        Assert.That(reconnectProbe.IsCompleted, Is.False);
    }

    [Test]
    [NonParallelizable]
    public async Task PriorityServers_auto_reconnect_proxy_disconnect_does_not_replay_current_command()
    {
        GaussDBGlobalClusterStatusTracker.Reset();
        GaussDBCoordinatorListTracker.Reset();

        await using var firstPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var secondPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var firstProxy = TcpFaultProxy.Start(firstPostmaster.Host, firstPostmaster.Port);
        await using var secondProxy = TcpFaultProxy.Start(secondPostmaster.Host, secondPostmaster.Port);

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(firstProxy, secondProxy),
            PriorityServers = 1,
            AutoReconnect = true,
            MaxReconnects = 2,
            Pooling = false,
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        };

        await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).BuildMultiHost();
        await using var conn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);

        Assert.That(conn.Port, Is.EqualTo(firstProxy.Port));
        _ = await firstPostmaster.WaitForServerConnection();

        await firstProxy.DisableAsync();
        var reconnectProbe = secondPostmaster.WaitForServerConnection().AsTask();

        Assert.ThrowsAsync<GaussDBException>(async () => await conn.ExecuteScalarAsync("SELECT 1"));
        Assert.That(conn.State, Is.EqualTo(ConnectionState.Closed));
        await Task.Delay(200);
        Assert.That(reconnectProbe.IsCompleted, Is.False);
    }

    [Test]
    [NonParallelizable]
    public async Task PriorityServers_auto_reconnect_disconnect_during_sql_execution_does_not_replay_current_command()
    {
        GaussDBGlobalClusterStatusTracker.Reset();
        GaussDBCoordinatorListTracker.Reset();

        await using var firstPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var secondPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var firstProxy = TcpFaultProxy.Start(firstPostmaster.Host, firstPostmaster.Port);
        await using var secondProxy = TcpFaultProxy.Start(secondPostmaster.Host, secondPostmaster.Port);

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(firstProxy, secondProxy),
            PriorityServers = 1,
            AutoReconnect = true,
            MaxReconnects = 2,
            Pooling = false,
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        };

        await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).BuildMultiHost();
        await using var conn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);

        Assert.That(conn.Port, Is.EqualTo(firstProxy.Port));
        var firstServer = await firstPostmaster.WaitForServerConnection();

        var queryTask = conn.ExecuteScalarAsync("SELECT 1");
        await firstServer.ExpectExtendedQuery();

        await firstProxy.DisableAsync();
        var reconnectProbe = secondPostmaster.WaitForServerConnection().AsTask();

        Assert.ThrowsAsync<GaussDBException>(async () => await queryTask);
        Assert.That(conn.State, Is.EqualTo(ConnectionState.Closed));
        await Task.Delay(200);
        Assert.That(reconnectProbe.IsCompleted, Is.False);
    }

    [Test]
    [NonParallelizable]
    public async Task PriorityServers_primary_az_single_cn_failure_routes_to_other_primary_az_cn()
    {
        GaussDBGlobalClusterStatusTracker.Reset();
        GaussDBCoordinatorListTracker.Reset();

        await using var az1FirstPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var az1SecondPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var az2FirstPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var az2SecondPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var az1FirstProxy = TcpFaultProxy.Start(az1FirstPostmaster.Host, az1FirstPostmaster.Port);
        await using var az1SecondProxy = TcpFaultProxy.Start(az1SecondPostmaster.Host, az1SecondPostmaster.Port);
        await using var az2FirstProxy = TcpFaultProxy.Start(az2FirstPostmaster.Host, az2FirstPostmaster.Port);
        await using var az2SecondProxy = TcpFaultProxy.Start(az2SecondPostmaster.Host, az2SecondPostmaster.Port);

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(az1FirstProxy, az1SecondProxy, az2FirstProxy, az2SecondProxy),
            PriorityServers = 2,
            Timeout = 2,
            Pooling = false,
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        };

        await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).BuildMultiHost();
        await using (var firstConnection = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any))
            Assert.That(firstConnection.Port, Is.EqualTo(az1FirstProxy.Port));

        az1FirstProxy.RejectNewConnections();
        await using var secondConnection = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);

        Assert.That(secondConnection.Port, Is.EqualTo(az1SecondProxy.Port));
    }

    [Test]
    [NonParallelizable]
    public async Task PriorityServers_failover_to_secondary_then_failback_to_primary_after_recovery()
    {
        GaussDBGlobalClusterStatusTracker.Reset();
        GaussDBCoordinatorListTracker.Reset();

        await using var az1FirstPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var az1SecondPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var az2FirstPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var az2SecondPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var az1FirstProxy = TcpFaultProxy.Start(az1FirstPostmaster.Host, az1FirstPostmaster.Port);
        await using var az1SecondProxy = TcpFaultProxy.Start(az1SecondPostmaster.Host, az1SecondPostmaster.Port);
        await using var az2FirstProxy = TcpFaultProxy.Start(az2FirstPostmaster.Host, az2FirstPostmaster.Port);
        await using var az2SecondProxy = TcpFaultProxy.Start(az2SecondPostmaster.Host, az2SecondPostmaster.Port);

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(az1FirstProxy, az1SecondProxy, az2FirstProxy, az2SecondProxy),
            PriorityServers = 2,
            Timeout = 2,
            Pooling = false,
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        };

        await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).BuildMultiHost();
        await using (var firstConnection = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any))
            Assert.That(firstConnection.Port, Is.EqualTo(az1FirstProxy.Port));

        az1FirstProxy.RejectNewConnections();
        az1SecondProxy.RejectNewConnections();
        await using (var failoverConnection = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any))
            Assert.That(new[] { az2FirstProxy.Port, az2SecondProxy.Port }, Contains.Item(failoverConnection.Port));

        az1FirstProxy.AcceptNewConnections();
        az1SecondProxy.AcceptNewConnections();

        az2FirstProxy.RejectNewConnections();
        az2SecondProxy.RejectNewConnections();
        dataSource.ClearDatabaseStates();
        await using (var failbackConnection = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any))
            Assert.That(new[] { az1FirstProxy.Port, az1SecondProxy.Port }, Contains.Item(failbackConnection.Port));
    }

    [Test]
    [NonParallelizable]
    public async Task PriorityServers_auto_reconnect_admin_shutdown_failover_to_secondary_then_failback_to_primary_after_recovery()
    {
        GaussDBGlobalClusterStatusTracker.Reset();
        GaussDBCoordinatorListTracker.Reset();

        await using var az1FirstPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var az1SecondPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var az2FirstPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var az2SecondPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var az1FirstProxy = TcpFaultProxy.Start(az1FirstPostmaster.Host, az1FirstPostmaster.Port);
        await using var az1SecondProxy = TcpFaultProxy.Start(az1SecondPostmaster.Host, az1SecondPostmaster.Port);
        await using var az2FirstProxy = TcpFaultProxy.Start(az2FirstPostmaster.Host, az2FirstPostmaster.Port);
        await using var az2SecondProxy = TcpFaultProxy.Start(az2SecondPostmaster.Host, az2SecondPostmaster.Port);

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(az1FirstProxy, az1SecondProxy, az2FirstProxy, az2SecondProxy),
            PriorityServers = 2,
            AutoReconnect = true,
            MaxReconnects = 3,
            Timeout = 2,
            Pooling = false,
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        };

        await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).BuildMultiHost();
        await using var conn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);

        Assert.That(conn.Port, Is.EqualTo(az1FirstProxy.Port));
        var az1CurrentServer = await az1FirstPostmaster.WaitForServerConnection();

        az1FirstProxy.RejectNewConnections();
        az1SecondProxy.RejectNewConnections();

        var failoverQueryTask = conn.ExecuteScalarAsync("SELECT 1");
        await az1CurrentServer.ExpectExtendedQuery();
        await az1CurrentServer
            .WriteErrorResponse(PostgresErrorCodes.AdminShutdown)
            .WriteReadyForQuery()
            .FlushAsync();
        var failoverTarget = await WaitForAnyServerConnection(
            (az2FirstProxy.Port, az2FirstPostmaster),
            (az2SecondProxy.Port, az2SecondPostmaster));
        await failoverTarget.Server.ExpectExtendedQuery();
        await failoverTarget.Server.WriteScalarResponseAndFlush(1);

        Assert.That(await failoverQueryTask, Is.EqualTo(1));
        Assert.That(new[] { az2FirstProxy.Port, az2SecondProxy.Port }, Contains.Item(conn.Port));
        Assert.That(new[] { az2FirstProxy.Port, az2SecondProxy.Port }, Contains.Item(failoverTarget.Port));

        az1FirstProxy.AcceptNewConnections();
        az1SecondProxy.AcceptNewConnections();
        dataSource.ClearDatabaseStates();

        az2FirstProxy.RejectNewConnections();
        az2SecondProxy.RejectNewConnections();

        var failbackQueryTask = conn.ExecuteScalarAsync("SELECT 2");
        await failoverTarget.Server.ExpectExtendedQuery();
        await failoverTarget.Server
            .WriteErrorResponse(PostgresErrorCodes.AdminShutdown)
            .WriteReadyForQuery()
            .FlushAsync();
        var failbackTarget = await WaitForAnyServerConnection(
            (az1FirstProxy.Port, az1FirstPostmaster),
            (az1SecondProxy.Port, az1SecondPostmaster));
        await failbackTarget.Server.ExpectExtendedQuery();
        await failbackTarget.Server.WriteScalarResponseAndFlush(2);

        Assert.That(await failbackQueryTask, Is.EqualTo(2));
        Assert.That(new[] { az1FirstProxy.Port, az1SecondProxy.Port }, Contains.Item(conn.Port));
        Assert.That(new[] { az1FirstProxy.Port, az1SecondProxy.Port }, Contains.Item(failbackTarget.Port));
    }

    [Test]
    [NonParallelizable]
    public async Task PriorityServers_exception_cn_eviction_admin_shutdown_auto_reconnect_and_az_failover()
    {
        GaussDBGlobalClusterStatusTracker.Reset();
        GaussDBCoordinatorListTracker.Reset();

        await using var az1FirstPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var az1SecondPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var az2FirstPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var az2SecondPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var az1FirstProxy = TcpFaultProxy.Start(az1FirstPostmaster.Host, az1FirstPostmaster.Port);
        await using var az1SecondProxy = TcpFaultProxy.Start(az1SecondPostmaster.Host, az1SecondPostmaster.Port);
        await using var az2FirstProxy = TcpFaultProxy.Start(az2FirstPostmaster.Host, az2FirstPostmaster.Port);
        await using var az2SecondProxy = TcpFaultProxy.Start(az2SecondPostmaster.Host, az2SecondPostmaster.Port);

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(az1FirstProxy, az1SecondProxy, az2FirstProxy, az2SecondProxy),
            PriorityServers = 2,
            AutoReconnect = true,
            MaxReconnects = 3,
            Timeout = 2,
            Pooling = false,
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        };

        await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).BuildMultiHost();
        var pools = dataSource.Pools;
        await using var conn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);

        Assert.That(conn.Port, Is.EqualTo(az1FirstProxy.Port));
        var az1FirstServer = await az1FirstPostmaster.WaitForServerConnection();

        az1FirstProxy.RejectNewConnections();

        var reconnectWithinPrimaryAzTask = conn.ExecuteScalarAsync("SELECT 1");
        await az1FirstServer.ExpectExtendedQuery();
        await az1FirstServer
            .WriteErrorResponse(PostgresErrorCodes.AdminShutdown)
            .WriteReadyForQuery()
            .FlushAsync();
        var reconnectWithinPrimaryAzServer = await az1SecondPostmaster.WaitForServerConnection();
        await reconnectWithinPrimaryAzServer.ExpectExtendedQuery();
        await reconnectWithinPrimaryAzServer.WriteScalarResponseAndFlush(1);

        Assert.That(await reconnectWithinPrimaryAzTask, Is.EqualTo(1));
        Assert.That(conn.Port, Is.EqualTo(az1SecondProxy.Port));
        Assert.That(pools[0].GetDatabaseState(ignoreExpiration: true), Is.EqualTo(DatabaseState.Offline));
        Assert.That(pools[1].GetDatabaseState(ignoreExpiration: true), Is.Not.EqualTo(DatabaseState.Offline));

        az1SecondProxy.RejectNewConnections();

        var failoverToSecondaryAzTask = conn.ExecuteScalarAsync("SELECT 2");
        await reconnectWithinPrimaryAzServer.ExpectExtendedQuery();
        await reconnectWithinPrimaryAzServer
            .WriteErrorResponse(PostgresErrorCodes.AdminShutdown)
            .WriteReadyForQuery()
            .FlushAsync();
        var failoverToSecondaryAzTarget = await WaitForAnyServerConnection(
            (az2FirstProxy.Port, az2FirstPostmaster),
            (az2SecondProxy.Port, az2SecondPostmaster));
        await failoverToSecondaryAzTarget.Server.ExpectExtendedQuery();
        await failoverToSecondaryAzTarget.Server.WriteScalarResponseAndFlush(2);

        Assert.That(await failoverToSecondaryAzTask, Is.EqualTo(2));
        Assert.That(new[] { az2FirstProxy.Port, az2SecondProxy.Port }, Contains.Item(failoverToSecondaryAzTarget.Port));
        Assert.That(new[] { az2FirstProxy.Port, az2SecondProxy.Port }, Contains.Item(conn.Port));
        Assert.That(pools[1].GetDatabaseState(ignoreExpiration: true), Is.EqualTo(DatabaseState.Offline));
    }

    [Test]
    [NonParallelizable]
    public async Task PriorityServers_retries_all_hosts_when_state_cache_marks_every_cluster_host_offline()
    {
        GaussDBGlobalClusterStatusTracker.Reset();
        GaussDBCoordinatorListTracker.Reset();

        await using var az1First = PgPostmasterMock.Start(state: Primary);
        await using var az1Second = PgPostmasterMock.Start(state: Primary);
        await using var az2First = PgPostmasterMock.Start(state: Primary);
        await using var az2Second = PgPostmasterMock.Start(state: Primary);

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(az1First, az1Second, az2First, az2Second),
            PriorityServers = 2,
            RefreshCNIpListTime = 30,
            Pooling = false,
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        };

        await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).BuildMultiHost();
        var pools = dataSource.Pools;
        var offlineSince = DateTime.UtcNow;
        foreach (var pool in pools)
            pool.UpdateDatabaseState(DatabaseState.Offline, offlineSince, TimeSpan.FromMinutes(1));

        var urlKey = ClusterKey(az1First, az1Second, az2First, az2Second);
        var az2ClusterKey = ClusterKey(az2First, az2Second);
        GaussDBGlobalClusterStatusTracker.ReportPrimary(urlKey, az2ClusterKey);

        await using var conn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);

        Assert.That(new[] { az2First.Port, az2Second.Port }, Contains.Item(conn.Port));
        Assert.That(pools[0].GetDatabaseState(ignoreExpiration: true), Is.EqualTo(DatabaseState.Offline));
        Assert.That(pools[1].GetDatabaseState(ignoreExpiration: true), Is.EqualTo(DatabaseState.Offline));
    }

    [Test]
    [NonParallelizable]
    public async Task Legacy_multi_host_retries_all_hosts_when_state_cache_marks_every_host_offline()
    {
        await using var first = PgPostmasterMock.Start(state: Primary);
        await using var second = PgPostmasterMock.Start(state: Primary);
        await using var third = PgPostmasterMock.Start(state: Primary);

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(first, second, third),
            Pooling = false,
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        };

        await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).BuildMultiHost();
        var offlineSince = DateTime.UtcNow;
        foreach (var pool in dataSource.Pools)
            pool.UpdateDatabaseState(DatabaseState.Offline, offlineSince, TimeSpan.FromMinutes(1));

        await using var connection = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);

        Assert.That(new[] { first.Port, second.Port, third.Port }, Contains.Item(connection.Port));
    }

    [Test]
    [NonParallelizable]
    public async Task PriorityServers_primary_az_recovers_with_readd_then_secondary_az_admin_shutdown_failure_fails_back_to_primary()
    {
        GaussDBGlobalClusterStatusTracker.Reset();
        GaussDBCoordinatorListTracker.Reset();

        await using var az1FirstPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var az1SecondPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var az2FirstPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var az2SecondPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var az1FirstProxy = TcpFaultProxy.Start(az1FirstPostmaster.Host, az1FirstPostmaster.Port);
        await using var az1SecondProxy = TcpFaultProxy.Start(az1SecondPostmaster.Host, az1SecondPostmaster.Port);
        await using var az2FirstProxy = TcpFaultProxy.Start(az2FirstPostmaster.Host, az2FirstPostmaster.Port);
        await using var az2SecondProxy = TcpFaultProxy.Start(az2SecondPostmaster.Host, az2SecondPostmaster.Port);

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(az1FirstProxy, az1SecondProxy, az2FirstProxy, az2SecondProxy),
            PriorityServers = 2,
            AutoReconnect = true,
            MaxReconnects = 3,
            Timeout = 2,
            Pooling = false,
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        };

        await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).BuildMultiHost();
        var pools = dataSource.Pools;
        await using var conn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);

        Assert.That(conn.Port, Is.EqualTo(az1FirstProxy.Port));
        var az1CurrentServer = await az1FirstPostmaster.WaitForServerConnection();

        az1FirstProxy.RejectNewConnections();
        az1SecondProxy.RejectNewConnections();

        var failoverQueryTask = conn.ExecuteScalarAsync("SELECT 1");
        await az1CurrentServer.ExpectExtendedQuery();
        await az1CurrentServer
            .WriteErrorResponse(PostgresErrorCodes.AdminShutdown)
            .WriteReadyForQuery()
            .FlushAsync();
        var failoverTarget = await WaitForAnyServerConnection(
            (az2FirstProxy.Port, az2FirstPostmaster),
            (az2SecondProxy.Port, az2SecondPostmaster));
        await failoverTarget.Server.ExpectExtendedQuery();
        await failoverTarget.Server.WriteScalarResponseAndFlush(1);

        Assert.That(await failoverQueryTask, Is.EqualTo(1));
        Assert.That(new[] { az2FirstProxy.Port, az2SecondProxy.Port }, Contains.Item(conn.Port));
        Assert.That(pools[0].GetDatabaseState(ignoreExpiration: true), Is.EqualTo(DatabaseState.Offline));
        Assert.That(pools[1].GetDatabaseState(ignoreExpiration: true), Is.EqualTo(DatabaseState.Offline));

        az1FirstProxy.AcceptNewConnections();
        az1SecondProxy.AcceptNewConnections();
        dataSource.ClearDatabaseStates();

        Assert.That(pools[0].GetDatabaseState(ignoreExpiration: true), Is.EqualTo(DatabaseState.Unknown));
        Assert.That(pools[1].GetDatabaseState(ignoreExpiration: true), Is.EqualTo(DatabaseState.Unknown));

        az2FirstProxy.RejectNewConnections();
        az2SecondProxy.RejectNewConnections();

        var failbackQueryTask = conn.ExecuteScalarAsync("SELECT 2");
        await failoverTarget.Server.ExpectExtendedQuery();
        await failoverTarget.Server
            .WriteErrorResponse(PostgresErrorCodes.AdminShutdown)
            .WriteReadyForQuery()
            .FlushAsync();
        var failbackTarget = await WaitForAnyServerConnection(
            (az1FirstProxy.Port, az1FirstPostmaster),
            (az1SecondProxy.Port, az1SecondPostmaster));
        await failbackTarget.Server.ExpectExtendedQuery();
        await failbackTarget.Server.WriteScalarResponseAndFlush(2);

        Assert.That(await failbackQueryTask, Is.EqualTo(2));
        Assert.That(new[] { az1FirstProxy.Port, az1SecondProxy.Port }, Contains.Item(conn.Port));
        Assert.That(new[] { az1FirstProxy.Port, az1SecondProxy.Port }, Contains.Item(failbackTarget.Port));
    }

    [Test]
    [NonParallelizable]
    public async Task PriorityServers_primary_az_recovers_after_host_recheck_then_secondary_az_admin_shutdown_failure_fails_back_to_primary()
    {
        GaussDBGlobalClusterStatusTracker.Reset();
        GaussDBCoordinatorListTracker.Reset();

        await using var az1FirstPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var az1SecondPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var az2FirstPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var az2SecondPostmaster = PgPostmasterMock.Start(state: Primary);
        await using var az1FirstProxy = TcpFaultProxy.Start(az1FirstPostmaster.Host, az1FirstPostmaster.Port);
        await using var az1SecondProxy = TcpFaultProxy.Start(az1SecondPostmaster.Host, az1SecondPostmaster.Port);
        await using var az2FirstProxy = TcpFaultProxy.Start(az2FirstPostmaster.Host, az2FirstPostmaster.Port);
        await using var az2SecondProxy = TcpFaultProxy.Start(az2SecondPostmaster.Host, az2SecondPostmaster.Port);

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(az1FirstProxy, az1SecondProxy, az2FirstProxy, az2SecondProxy),
            PriorityServers = 2,
            AutoReconnect = true,
            MaxReconnects = 3,
            Timeout = 2,
            HostRecheckSeconds = 1,
            Pooling = false,
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        };

        await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).BuildMultiHost();
        var pools = dataSource.Pools;
        await using var conn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);

        Assert.That(conn.Port, Is.EqualTo(az1FirstProxy.Port));
        var az1CurrentServer = await az1FirstPostmaster.WaitForServerConnection();

        az1FirstProxy.RejectNewConnections();
        az1SecondProxy.RejectNewConnections();

        var failoverQueryTask = conn.ExecuteScalarAsync("SELECT 1");
        await az1CurrentServer.ExpectExtendedQuery();
        await az1CurrentServer
            .WriteErrorResponse(PostgresErrorCodes.AdminShutdown)
            .WriteReadyForQuery()
            .FlushAsync();
        var failoverTarget = await WaitForAnyServerConnection(
            (az2FirstProxy.Port, az2FirstPostmaster),
            (az2SecondProxy.Port, az2SecondPostmaster));
        await failoverTarget.Server.ExpectExtendedQuery();
        await failoverTarget.Server.WriteScalarResponseAndFlush(1);

        Assert.That(await failoverQueryTask, Is.EqualTo(1));
        Assert.That(new[] { az2FirstProxy.Port, az2SecondProxy.Port }, Contains.Item(conn.Port));
        Assert.That(pools[0].GetDatabaseState(ignoreExpiration: true), Is.EqualTo(DatabaseState.Offline));
        Assert.That(pools[1].GetDatabaseState(ignoreExpiration: true), Is.EqualTo(DatabaseState.Offline));

        az1FirstProxy.AcceptNewConnections();
        az1SecondProxy.AcceptNewConnections();

        await Task.Delay(TimeSpan.FromSeconds(2));

        Assert.That(pools[0].GetDatabaseState(), Is.EqualTo(DatabaseState.Unknown));
        Assert.That(pools[1].GetDatabaseState(), Is.EqualTo(DatabaseState.Unknown));

        az2FirstProxy.RejectNewConnections();
        az2SecondProxy.RejectNewConnections();

        var failbackQueryTask = conn.ExecuteScalarAsync("SELECT 2");
        await failoverTarget.Server.ExpectExtendedQuery();
        await failoverTarget.Server
            .WriteErrorResponse(PostgresErrorCodes.AdminShutdown)
            .WriteReadyForQuery()
            .FlushAsync();
        var failbackTarget = await WaitForAnyServerConnection(
            (az1FirstProxy.Port, az1FirstPostmaster),
            (az1SecondProxy.Port, az1SecondPostmaster));
        await failbackTarget.Server.ExpectExtendedQuery();
        await failbackTarget.Server.WriteScalarResponseAndFlush(2);

        Assert.That(await failbackQueryTask, Is.EqualTo(2));
        Assert.That(new[] { az1FirstProxy.Port, az1SecondProxy.Port }, Contains.Item(conn.Port));
        Assert.That(new[] { az1FirstProxy.Port, az1SecondProxy.Port }, Contains.Item(failbackTarget.Port));
    }

    [Test]
    [NonParallelizable]
    public async Task TcpFaultProxy_disconnect_existing_connections_preserves_new_connections()
    {
        await using var postmaster = PgPostmasterMock.Start(state: Primary);
        await using var proxy = TcpFaultProxy.Start(postmaster.Host, postmaster.Port);

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = proxy.Endpoint,
            Pooling = false,
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        };

        await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).Build();
        await using var firstConnection = await dataSource.OpenConnectionAsync();
        _ = await postmaster.WaitForServerConnection();

        Assert.That(proxy.ActiveConnectionCount, Is.EqualTo(1));
        proxy.DisconnectExistingConnections();

        Assert.ThrowsAsync<GaussDBException>(() => firstConnection.ExecuteScalarAsync("SELECT 1"));

        await using var secondConnection = await dataSource.OpenConnectionAsync();
        Assert.That(secondConnection.Port, Is.EqualTo(proxy.Port));
        _ = await postmaster.WaitForServerConnection();
    }

    [Test]
    [NonParallelizable]
    public async Task TcpFaultProxy_reject_new_connections_can_be_restored()
    {
        await using var postmaster = PgPostmasterMock.Start(state: Primary);
        await using var proxy = TcpFaultProxy.Start(postmaster.Host, postmaster.Port);

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = proxy.Endpoint,
            Timeout = 2,
            Pooling = false,
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        };

        proxy.RejectNewConnections();

        AsyncTestDelegate openWhileRejected = async () =>
        {
            await using var rejectedConnection = new GaussDBConnection(builder.ConnectionString);
            await rejectedConnection.OpenAsync();
        };

        Assert.ThrowsAsync<GaussDBException>(openWhileRejected);

        proxy.AcceptNewConnections();

        await using var restoredConnection = new GaussDBConnection(builder.ConnectionString);
        await restoredConnection.OpenAsync();
        Assert.That(restoredConnection.Port, Is.EqualTo(proxy.Port));
        _ = await postmaster.WaitForServerConnection();
    }

    [Test, NonParallelizable]
    public void IntegrationTest([Values] bool loadBalancing, [Values] bool alwaysCheckHostState)
    {
        PoolManager.Reset();

        var dataSourceBuilder = new GaussDBDataSourceBuilder(ConnectionString)
        {
            ConnectionStringBuilder =
            {
                Host = "localhost,127.0.0.1",
                Pooling = true,
                MaxPoolSize = 2,
                LoadBalanceHosts = loadBalancing,
                HostRecheckSeconds = alwaysCheckHostState ? 0 : 10,
            }
        };
        using var dataSource = dataSourceBuilder.BuildMultiHost();

        var queriesDone = 0;

        var clientsTask = Task.WhenAll(
            Client(dataSource, TargetSessionAttributes.Any),
            Client(dataSource, TargetSessionAttributes.Primary),
            Client(dataSource, TargetSessionAttributes.PreferPrimary),
            Client(dataSource, TargetSessionAttributes.PreferStandby),
            Client(dataSource, TargetSessionAttributes.ReadWrite));

        var onlyStandbyClient = Client(dataSource, TargetSessionAttributes.Standby);
        var readOnlyClient = Client(dataSource, TargetSessionAttributes.ReadOnly);

        Assert.DoesNotThrowAsync(() => clientsTask);
        Assert.ThrowsAsync<GaussDBException>(() => onlyStandbyClient);
        Assert.ThrowsAsync<GaussDBException>(() => readOnlyClient);
        Assert.AreEqual(125, queriesDone);

        Task Client(GaussDBMultiHostDataSource multiHostDataSource, TargetSessionAttributes targetSessionAttributes)
        {
            var dataSource = multiHostDataSource.WithTargetSession(targetSessionAttributes);
            var tasks = new List<Task>(5);

            for (var i = 0; i < 5; i++)
            {
                tasks.Add(Task.Run(() => Query(dataSource)));
            }

            return Task.WhenAll(tasks);
        }

        async Task Query(GaussDBDataSource dataSource)
        {
            await using var conn = dataSource.CreateConnection();
            for (var i = 0; i < 5; i++)
            {
                await conn.OpenAsync();
                await conn.ExecuteNonQueryAsync("SELECT 1");
                await conn.CloseAsync();
                Interlocked.Increment(ref queriesDone);
            }
        }
    }

    [Test]
    [IssueLink("https://github.com/npgsql/npgsql/issues/5055")]
    [NonParallelizable] // Disables sql rewriting
    public async Task Multiple_hosts_with_disabled_sql_rewriting()
    {
        using var _ = DisableSqlRewriting();

        var dataSourceBuilder = new GaussDBDataSourceBuilder(ConnectionString)
        {
            ConnectionStringBuilder =
            {
                Host = "localhost,127.0.0.1",
                Pooling = true,
                HostRecheckSeconds = 0
            }
        };
        await using var dataSource = dataSourceBuilder.BuildMultiHost();
        await using var conn = await dataSource.OpenConnectionAsync();
    }

    [Test]
    public async Task DataSource_with_wrappers()
    {
        await using var primaryPostmasterMock = PgPostmasterMock.Start(state: Primary);
        await using var standbyPostmasterMock = PgPostmasterMock.Start(state: Standby);

        var builder = new GaussDBDataSourceBuilder
        {
            ConnectionStringBuilder =
            {
                Host = MultipleHosts(primaryPostmasterMock, standbyPostmasterMock),
                ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading,
            }
        };

        await using var dataSource = builder.BuildMultiHost();
        await using var primaryDataSource = dataSource.WithTargetSession(TargetSessionAttributes.Primary);
        await using var standbyDataSource = dataSource.WithTargetSession(TargetSessionAttributes.Standby);

        await using var primaryConnection = await primaryDataSource.OpenConnectionAsync();
        Assert.That(primaryConnection.Port, Is.EqualTo(primaryPostmasterMock.Port));

        await using var standbyConnection = await standbyDataSource.OpenConnectionAsync();
        Assert.That(standbyConnection.Port, Is.EqualTo(standbyPostmasterMock.Port));
    }

    [Test]
    public async Task DataSource_without_wrappers()
    {
        await using var primaryPostmasterMock = PgPostmasterMock.Start(state: Primary);
        await using var standbyPostmasterMock = PgPostmasterMock.Start(state: Standby);

        var builder = new GaussDBDataSourceBuilder
        {
            ConnectionStringBuilder =
            {
                Host = MultipleHosts(primaryPostmasterMock, standbyPostmasterMock),
                ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading,
            }
        };

        await using var dataSource = builder.BuildMultiHost();

        await using var primaryConnection = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Primary);
        Assert.That(primaryConnection.Port, Is.EqualTo(primaryPostmasterMock.Port));

        await using var standbyConnection = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Standby);
        Assert.That(standbyConnection.Port, Is.EqualTo(standbyPostmasterMock.Port));
    }

    [Test]
    public void DataSource_with_TargetSessionAttributes_is_not_supported()
    {
        var builder = new GaussDBDataSourceBuilder("Host=foo,bar;Target Session Attributes=primary");

        Assert.That(() => builder.BuildMultiHost(), Throws.Exception.TypeOf<InvalidOperationException>()
            .With.Message.EqualTo(GaussDBStrings.CannotSpecifyTargetSessionAttributes));
    }

    [Test]
    public async Task BuildMultiHost_with_single_host_is_supported()
    {
        var builder = new GaussDBDataSourceBuilder(ConnectionString);
        await using var dataSource = builder.BuildMultiHost();
        await using var connection = await dataSource.OpenConnectionAsync();
        Assert.That(await connection.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
    }

    [Test]
    public async Task Build_with_multiple_hosts_is_supported()
    {
        await using var primaryPostmasterMock = PgPostmasterMock.Start(state: Primary);
        await using var standbyPostmasterMock = PgPostmasterMock.Start(state: Standby);

        var builder = new GaussDBDataSourceBuilder
        {
            ConnectionStringBuilder =
            {
                Host = MultipleHosts(primaryPostmasterMock, standbyPostmasterMock),
                ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading,
            }
        };

        await using var dataSource = builder.Build();
        await using var connection = await dataSource.OpenConnectionAsync();
    }

    [Test]
    [NonParallelizable]
    public async Task PriorityServers_prefers_discovered_primary_cluster()
    {
        GaussDBGlobalClusterStatusTracker.Reset();
        GaussDBCoordinatorListTracker.Reset();

        await using var az1First = PgPostmasterMock.Start(state: Standby);
        await using var az1Second = PgPostmasterMock.Start(state: Standby);
        await using var az2First = PgPostmasterMock.Start(state: Primary);
        await using var az2Second = PgPostmasterMock.Start(state: Primary);

        SeedCoordinatorSnapshot(az1First, az1Second);
        SeedCoordinatorSnapshot(az2First, az2Second);

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(az1First, az1Second, az2First, az2Second),
            PriorityServers = 2,
            Pooling = false,
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        };

        await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).BuildMultiHost();

        await using (var firstConnection = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Primary))
            Assert.That(new[] { az2First.Port, az2Second.Port }, Contains.Item(firstConnection.Port));

        az1First.State = Primary;
        az1Second.State = Primary;

        await using (var secondConnection = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Primary))
            Assert.That(new[] { az2First.Port, az2Second.Port }, Contains.Item(secondConnection.Port));
    }

    [Test]
    [NonParallelizable]
    public async Task AutoBalance_priority_prefers_priority_seed_subset()
    {
        GaussDBGlobalClusterStatusTracker.Reset();
        GaussDBCoordinatorListTracker.Reset();

        await using var first = PgPostmasterMock.Start(state: Primary);
        await using var second = PgPostmasterMock.Start(state: Primary);
        await using var third = PgPostmasterMock.Start(state: Primary);

        SeedCoordinatorSnapshot(first, second, third);

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(first, second, third),
            AutoBalance = "priority2",
            Pooling = false,
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        };

        await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).BuildMultiHost();
        for (var i = 0; i < 4; i++)
        {
            await using var connection = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);
            Assert.That(connection.Port, Is.Not.EqualTo(third.Port));
        }
    }

    [Test]
    [NonParallelizable]
    public async Task PriorityServers_autobalance_falls_back_to_secondary_seed_cluster_when_preferred_snapshot_is_unreachable()
    {
        GaussDBGlobalClusterStatusTracker.Reset();
        GaussDBCoordinatorListTracker.Reset();

        await using var az1First = PgPostmasterMock.Start(state: Primary);
        await using var az1Second = PgPostmasterMock.Start(state: Primary);
        await using var az2First = PgPostmasterMock.Start(state: Primary);
        await using var az2Second = PgPostmasterMock.Start(state: Primary);

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(az1First, az1Second, az2First, az2Second),
            PriorityServers = 2,
            AutoBalance = "priority2",
            RefreshCNIpListTime = 30,
            Pooling = false,
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        };

        var az1ClusterKey = ClusterKey(az1First, az1Second);
        var az2ClusterKey = ClusterKey(az2First, az2Second);
        GaussDBGlobalClusterStatusTracker.ReportPrimary(ClusterKey(az1First, az1Second, az2First, az2Second), az1ClusterKey);

        var unreachableEndpoints = new[]
        {
            GetUnreachableEndpoint(),
            GetUnreachableEndpoint(),
            GetUnreachableEndpoint()
        };

        // 两个簇都预置同一批不可达 CN，验证备用簇仍然必须退回自己的 seed hosts，而不是继续信任快照。
        GaussDBCoordinatorListTracker.SeedSnapshotForTesting(az1ClusterKey, unreachableEndpoints);
        GaussDBCoordinatorListTracker.SeedSnapshotForTesting(az2ClusterKey, unreachableEndpoints);

        await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).BuildMultiHost();
        await using var connection = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);

        Assert.That(new[] { az2First.Port, az2Second.Port }, Contains.Item(connection.Port));
    }

    [Test]
    [NonParallelizable]
    public async Task AutoBalance_priority_falls_back_to_seed_hosts_when_snapshot_is_unreachable()
    {
        GaussDBGlobalClusterStatusTracker.Reset();
        GaussDBCoordinatorListTracker.Reset();

        await using var first = PgPostmasterMock.Start(state: Primary);
        await using var second = PgPostmasterMock.Start(state: Primary);
        await using var third = PgPostmasterMock.Start(state: Primary);

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(first, second, third),
            AutoBalance = "priority2",
            RefreshCNIpListTime = 30,
            Pooling = false,
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        };

        GaussDBCoordinatorListTracker.SeedSnapshotForTesting(
            ClusterKey(first, second, third),
            GetUnreachableEndpoint(),
            GetUnreachableEndpoint(),
            GetUnreachableEndpoint());

        await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).BuildMultiHost();
        await using var connection = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);

        Assert.That(new[] { first.Port, second.Port }, Contains.Item(connection.Port));
    }

    [Test]
    public async Task Coordinator_snapshot_refresh_failure_is_throttled_by_refresh_interval()
    {
        GaussDBCoordinatorListTracker.Reset();

        var refreshAttempts = 0;

        async ValueTask<HaCoordinatorNode[]?> Refresh(CancellationToken cancellationToken)
        {
            refreshAttempts++;
            await Task.CompletedTask;
            throw new InvalidOperationException("boom");
        }

        var clusterKey = "cluster-a";
        var refreshInterval = TimeSpan.FromMinutes(1);

        var first = await GaussDBCoordinatorListTracker.GetSnapshotAsync(
            clusterKey,
            refreshInterval,
            Refresh,
            async: true,
            CancellationToken.None);
        var second = await GaussDBCoordinatorListTracker.GetSnapshotAsync(
            clusterKey,
            refreshInterval,
            Refresh,
            async: true,
            CancellationToken.None);

        Assert.That(first, Is.Null);
        Assert.That(second, Is.Null);
        Assert.That(refreshAttempts, Is.EqualTo(1));
    }

    [Test]
    [NonParallelizable]
    public async Task Dynamic_endpoint_is_preferred_before_seed_for_same_node()
    {
        GaussDBCoordinatorListTracker.Reset();

        await using var seed = PgPostmasterMock.Start(state: Primary);
        await using var dynamic = PgPostmasterMock.Start(state: Primary);

        GaussDBCoordinatorListTracker.SeedSnapshotForTesting(
            ClusterKey(seed),
            CoordinatorNode("cn_1", seed, dynamic));

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(seed),
            AutoBalance = "shuffle",
            RefreshCNIpListTime = 30,
            UsingEip = true,
            Pooling = false,
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        };

        await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).BuildMultiHost();
        await using var connection = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);

        Assert.That(connection.Port, Is.EqualTo(dynamic.Port));
    }

    [Test]
    [NonParallelizable]
    public async Task Dynamic_endpoint_failure_falls_back_to_seed_for_same_node()
    {
        GaussDBCoordinatorListTracker.Reset();

        await using var seed = PgPostmasterMock.Start(state: Primary);
        var unreachable = GetUnreachableEndpoint();

        GaussDBCoordinatorListTracker.SeedSnapshotForTesting(
            ClusterKey(seed),
            new HaCoordinatorNode("cn_1", new HaEndpoint(seed.Host, seed.Port), unreachable));

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(seed),
            AutoBalance = "shuffle",
            RefreshCNIpListTime = 30,
            UsingEip = true,
            Pooling = false,
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        };

        await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).BuildMultiHost();
        await using var connection = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);

        Assert.That(connection.Port, Is.EqualTo(seed.Port));
    }

    [Test]
    [NonParallelizable]
    public async Task Unknown_discovered_node_is_added_to_refreshing_cluster()
    {
        GaussDBCoordinatorListTracker.Reset();

        var seedEndpoint = GetUnreachableEndpoint();
        await using var expanded = PgPostmasterMock.Start(state: Primary);
        var expandedEndpoint = new HaEndpoint(expanded.Host, expanded.Port);

        GaussDBCoordinatorListTracker.SeedSnapshotForTesting(
            seedEndpoint.Key,
            new HaCoordinatorNode("cn_seed", seedEndpoint, seedEndpoint),
            new HaCoordinatorNode("cn_expanded", expandedEndpoint, expandedEndpoint));

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = seedEndpoint.Key,
            AutoBalance = "roundrobin",
            RefreshCNIpListTime = 30,
            Pooling = false,
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        };

        await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).BuildMultiHost();
        await using var connection = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);

        Assert.That(connection.Port, Is.EqualTo(expanded.Port));
    }

    [Test]
    [NonParallelizable]
    public async Task Refreshing_primary_cluster_updates_other_cluster_dynamic_endpoint_without_joining_current_cluster()
    {
        GaussDBGlobalClusterStatusTracker.Reset();
        GaussDBCoordinatorListTracker.Reset();

        await using var az2Seed = PgPostmasterMock.Start(state: Primary);
        await using var az2Dynamic = PgPostmasterMock.Start(state: Primary);
        var az1Seed = GetUnreachableEndpoint();

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = $"{az1Seed.Key},{az2Seed.Host}:{az2Seed.Port}",
            PriorityServers = 1,
            AutoBalance = "shuffle",
            RefreshCNIpListTime = 30,
            UsingEip = true,
            Pooling = false,
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        };

        var urlKey = string.Join(",", new[] { az1Seed.Key, $"{az2Seed.Host}:{az2Seed.Port}" }.OrderBy(static x => x, StringComparer.Ordinal));
        var az1ClusterKey = az1Seed.Key;
        var az2ClusterKey = ClusterKey(az2Seed);
        GaussDBGlobalClusterStatusTracker.ReportPrimary(urlKey, az1ClusterKey);
        GaussDBCoordinatorListTracker.SeedSnapshotForTesting(
            az1ClusterKey,
            new HaCoordinatorNode("cn_1", az1Seed, az1Seed),
            CoordinatorNode("cn_2", az2Seed, az2Dynamic));

        await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).BuildMultiHost();
        await using var connection = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);

        Assert.That(connection.Port, Is.EqualTo(az2Dynamic.Port));
        Assert.That(GaussDBGlobalClusterStatusTracker.GetPreferredClusterKey(urlKey), Is.EqualTo(az2ClusterKey));
    }

    [Test]
    [NonParallelizable]
    public async Task Preferred_cluster_uses_same_node_seed_fallback_before_secondary_cluster_when_seed_binding_comes_from_get_nodename()
    {
        GaussDBGlobalClusterStatusTracker.Reset();
        GaussDBCoordinatorListTracker.Reset();

        await using var az1First = PgPostmasterMock.Start(state: Primary);
        await using var az1Second = PgPostmasterMock.Start(state: Primary);
        await using var az2First = PgPostmasterMock.Start(state: Primary);

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(az1First, az1Second, az2First),
            PriorityServers = 2,
            AutoBalance = "priority2",
            RefreshCNIpListTime = 30,
            UsingEip = true,
            Pooling = false,
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        };

        var az1ClusterKey = ClusterKey(az1First, az1Second);
        var urlKey = ClusterKey(az1First, az1Second, az2First);
        GaussDBGlobalClusterStatusTracker.ReportPrimary(urlKey, az1ClusterKey);

        GaussDBCoordinatorListTracker.SeedSnapshotForTesting(
            az1ClusterKey,
            new HaCoordinatorNode("cn_5001", GetUnreachableEndpoint(), GetUnreachableEndpoint()),
            new HaCoordinatorNode("cn_5002", GetUnreachableEndpoint(), GetUnreachableEndpoint()),
            new HaCoordinatorNode("cn_5003", GetUnreachableEndpoint(), GetUnreachableEndpoint()));

        await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).BuildMultiHost();

        var seedProbe1 = RespondToGetNodeName(az1First, "cn_5001");
        var seedProbe2 = RespondToGetNodeName(az1Second, "cn_5002");
        var seedProbe3 = RespondToGetNodeName(az2First, "cn_5003");

        await using var connection = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);

        Assert.That(new[] { az1First.Port, az1Second.Port }, Contains.Item(connection.Port));

        await Task.WhenAll(seedProbe1, seedProbe2, seedProbe3);
    }

    [Test]
    [NonParallelizable]
    public async Task Standby_connection_does_not_overwrite_preferred_primary_cluster()
    {
        GaussDBGlobalClusterStatusTracker.Reset();
        GaussDBCoordinatorListTracker.Reset();

        await using var az1First = PgPostmasterMock.Start(state: Primary);
        await using var az1Second = PgPostmasterMock.Start(state: Primary);
        await using var az2First = PgPostmasterMock.Start(state: Standby);
        await using var az2Second = PgPostmasterMock.Start(state: Standby);

        SeedCoordinatorSnapshot(az1First, az1Second);
        SeedCoordinatorSnapshot(az2First, az2Second);

        var builder = new GaussDBConnectionStringBuilder
        {
            Host = MultipleHosts(az1First, az1Second, az2First, az2Second),
            PriorityServers = 2,
            Pooling = false,
            ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading
        };

        var urlKey = ClusterKey(az1First, az1Second, az2First, az2Second);
        var az1ClusterKey = ClusterKey(az1First, az1Second);
        GaussDBGlobalClusterStatusTracker.ReportPrimary(urlKey, az1ClusterKey);

        await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).BuildMultiHost();
        await using var standbyConnection = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Standby);

        Assert.That(new[] { az2First.Port, az2Second.Port }, Contains.Item(standbyConnection.Port));
        Assert.That(GaussDBGlobalClusterStatusTracker.GetPreferredClusterKey(urlKey), Is.EqualTo(az1ClusterKey));
    }

    [Test, IssueLink("https://github.com/npgsql/npgsql/issues/4181")]
    [Explicit("Fails until #4181 is fixed.")]
    public async Task LoadBalancing_is_fair_if_first_host_is_down([Values]TargetSessionAttributes targetSessionAttributes)
    {
        await using var pDown = PgPostmasterMock.Start(state: Primary, startupErrorCode: PostgresErrorCodes.CannotConnectNow);
        await using var pRw1 = PgPostmasterMock.Start(state: Primary);
        await using var pR1 = PgPostmasterMock.Start(state: PrimaryReadOnly);
        await using var s1 = PgPostmasterMock.Start(state: Standby);
        await using var pRw2 = PgPostmasterMock.Start(state: Primary);
        await using var pR2 = PgPostmasterMock.Start(state: PrimaryReadOnly);
        await using var s2 = PgPostmasterMock.Start(state: Standby);

        var hostList = $"{pDown.Host}:{pDown.Port}," +
                       $"{pRw1.Host}:{pRw1.Port}," +
                       $"{pR1.Host}:{pR1.Port}," +
                       $"{s1.Host}:{s1.Port}," +
                       $"{pRw2.Host}:{pRw2.Port}," +
                       $"{pR2.Host}:{pR2.Port}," +
                       $"{s2.Host}:{s2.Port}";

        await using var dataSource = CreateDataSource(builder =>
        {
            builder.Host = hostList;
            builder.ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading;
            builder.LoadBalanceHosts = true;
            builder.TargetSessionAttributesParsed = targetSessionAttributes;

        });
        var connections = Enumerable.Repeat(0, 12).Select(_ => dataSource.OpenConnection()).ToArray();
        await using var __ = new DisposableWrapper(connections);

        switch (targetSessionAttributes)
        {
        case TargetSessionAttributes.Any:
            Assert.That(connections[0].Port, Is.EqualTo(pRw1.Port));
            Assert.That(connections[1].Port, Is.EqualTo(pR1.Port));
            Assert.That(connections[2].Port, Is.EqualTo(s1.Port));
            Assert.That(connections[3].Port, Is.EqualTo(pRw2.Port));
            Assert.That(connections[4].Port, Is.EqualTo(pR2.Port));
            Assert.That(connections[5].Port, Is.EqualTo(s2.Port));
            Assert.That(connections[6].Port, Is.EqualTo(pRw1.Port));
            Assert.That(connections[7].Port, Is.EqualTo(pR1.Port));
            Assert.That(connections[8].Port, Is.EqualTo(s1.Port));
            Assert.That(connections[9].Port, Is.EqualTo(pRw2.Port));
            Assert.That(connections[10].Port, Is.EqualTo(pR2.Port));
            Assert.That(connections[11].Port, Is.EqualTo(s2.Port));
            break;
        case TargetSessionAttributes.ReadWrite:
            Assert.That(connections[0].Port, Is.EqualTo(pRw1.Port));
            Assert.That(connections[1].Port, Is.EqualTo(pRw2.Port));
            Assert.That(connections[2].Port, Is.EqualTo(pRw1.Port));
            Assert.That(connections[3].Port, Is.EqualTo(pRw2.Port));
            Assert.That(connections[4].Port, Is.EqualTo(pRw1.Port));
            Assert.That(connections[5].Port, Is.EqualTo(pRw2.Port));
            Assert.That(connections[6].Port, Is.EqualTo(pRw1.Port));
            Assert.That(connections[7].Port, Is.EqualTo(pRw2.Port));
            Assert.That(connections[8].Port, Is.EqualTo(pRw1.Port));
            Assert.That(connections[9].Port, Is.EqualTo(pRw2.Port));
            Assert.That(connections[10].Port, Is.EqualTo(pRw1.Port));
            Assert.That(connections[11].Port, Is.EqualTo(pRw2.Port));
            break;
        case TargetSessionAttributes.ReadOnly:
            Assert.That(connections[0].Port, Is.EqualTo(pR1.Port));
            Assert.That(connections[1].Port, Is.EqualTo(s1.Port));
            Assert.That(connections[2].Port, Is.EqualTo(pR2.Port));
            Assert.That(connections[3].Port, Is.EqualTo(s2.Port));
            Assert.That(connections[4].Port, Is.EqualTo(pR1.Port));
            Assert.That(connections[5].Port, Is.EqualTo(s1.Port));
            Assert.That(connections[6].Port, Is.EqualTo(pR2.Port));
            Assert.That(connections[7].Port, Is.EqualTo(s2.Port));
            Assert.That(connections[8].Port, Is.EqualTo(pR1.Port));
            Assert.That(connections[9].Port, Is.EqualTo(s1.Port));
            Assert.That(connections[10].Port, Is.EqualTo(pR2.Port));
            Assert.That(connections[11].Port, Is.EqualTo(s2.Port));
            break;
        case TargetSessionAttributes.Primary:
        case TargetSessionAttributes.PreferPrimary:
            Assert.That(connections[0].Port, Is.EqualTo(pRw1.Port));
            Assert.That(connections[1].Port, Is.EqualTo(pR1.Port));
            Assert.That(connections[2].Port, Is.EqualTo(pRw2.Port));
            Assert.That(connections[3].Port, Is.EqualTo(pR2.Port));
            Assert.That(connections[4].Port, Is.EqualTo(pRw1.Port));
            Assert.That(connections[5].Port, Is.EqualTo(pR1.Port));
            Assert.That(connections[6].Port, Is.EqualTo(pRw2.Port));
            Assert.That(connections[7].Port, Is.EqualTo(pR2.Port));
            Assert.That(connections[8].Port, Is.EqualTo(pRw1.Port));
            Assert.That(connections[9].Port, Is.EqualTo(pR1.Port));
            Assert.That(connections[10].Port, Is.EqualTo(pRw2.Port));
            Assert.That(connections[11].Port, Is.EqualTo(pR2.Port));
            break;
        case TargetSessionAttributes.Standby:
        case TargetSessionAttributes.PreferStandby:
            Assert.That(connections[0].Port, Is.EqualTo(s1.Port));
            Assert.That(connections[1].Port, Is.EqualTo(s2.Port));
            Assert.That(connections[2].Port, Is.EqualTo(s1.Port));
            Assert.That(connections[3].Port, Is.EqualTo(s2.Port));
            Assert.That(connections[4].Port, Is.EqualTo(s1.Port));
            Assert.That(connections[5].Port, Is.EqualTo(s2.Port));
            Assert.That(connections[6].Port, Is.EqualTo(s1.Port));
            Assert.That(connections[7].Port, Is.EqualTo(s2.Port));
            Assert.That(connections[8].Port, Is.EqualTo(s1.Port));
            Assert.That(connections[9].Port, Is.EqualTo(s2.Port));
            Assert.That(connections[10].Port, Is.EqualTo(s1.Port));
            Assert.That(connections[11].Port, Is.EqualTo(s2.Port));
            break;
        }
    }

    static string MultipleHosts(params PgPostmasterMock[] postmasters)
        => string.Join(",", postmasters.Select(p => $"{p.Host}:{p.Port}"));

    static string MultipleHosts(params TcpFaultProxy[] proxies)
        => string.Join(",", proxies.Select(p => p.Endpoint));

    static string ClusterKey(params PgPostmasterMock[] postmasters)
        => string.Join(",", postmasters.Select(p => $"{p.Host}:{p.Port}").OrderBy(static endpoint => endpoint, StringComparer.Ordinal));

    static void SeedCoordinatorSnapshot(params PgPostmasterMock[] postmasters)
        => GaussDBCoordinatorListTracker.SeedSnapshotForTesting(
            ClusterKey(postmasters),
            postmasters.Select(p => new HaEndpoint(p.Host, p.Port)).ToArray());

    static HaCoordinatorNode CoordinatorNode(string nodeName, PgPostmasterMock hostEndpoint, PgPostmasterMock eipEndpoint)
        => new(
            nodeName,
            new HaEndpoint(hostEndpoint.Host, hostEndpoint.Port),
            new HaEndpoint(eipEndpoint.Host, eipEndpoint.Port));

    static async Task RespondToGetNodeName(PgPostmasterMock postmaster, string nodeName)
    {
        var server = await postmaster.WaitForServerConnection();
        await server.ExpectExtendedQuery();
        await server.WriteScalarResponseAndFlush(nodeName);
    }

    static HaEndpoint GetUnreachableEndpoint()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return new(IPAddress.Loopback.ToString(), port);
    }

    static async Task<(int Port, PgServerMock Server)> WaitForAnyServerConnection(params (int Port, PgPostmasterMock Postmaster)[] candidates)
    {
        var candidateTasks = candidates
            .Select(async candidate => (candidate.Port, Server: await candidate.Postmaster.WaitForServerConnection()))
            .ToArray();

        var timeoutTask = Task.Delay(TimeSpan.FromSeconds(10));
        var completedTask = await Task.WhenAny(candidateTasks.Cast<Task>().Append(timeoutTask));
        if (completedTask == timeoutTask)
            throw new TimeoutException("Timed out waiting for a server connection on any candidate postmaster.");

        return await (Task<(int Port, PgServerMock Server)>)completedTask;
    }

    class DisposableWrapper(IEnumerable<IAsyncDisposable> disposables) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            foreach (var disposable in disposables)
                await disposable.DisposeAsync();
        }
    }
}
