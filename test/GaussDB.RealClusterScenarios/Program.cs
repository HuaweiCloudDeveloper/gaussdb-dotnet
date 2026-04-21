using System.Collections.Concurrent;
using System.Data;
using System.Net;
using System.Net.Sockets;
using HuaweiCloud.GaussDB;

var options = Options.Parse(args);

switch (options.Mode)
{
case "list":
    PrintScenarioList();
    return;
case "open-failover":
    await RunScenarioAsync("open-failover", () => OpenFailoverAsync(options));
    return;
case "admin-shutdown-replay":
    await RunScenarioAsync("admin-shutdown-replay", () => AdminShutdownReplayAsync(options));
    return;
case "proxy-disconnect-no-replay":
    await RunScenarioAsync("proxy-disconnect-no-replay", () => ProxyDisconnectNoReplayAsync(options));
    return;
case "explicit-tx-admin-shutdown-no-replay":
    await RunScenarioAsync("explicit-tx-admin-shutdown-no-replay", () => ExplicitTransactionNoReplayAsync(options));
    return;
case "active-reader-disconnect-no-replay":
    await RunScenarioAsync("active-reader-disconnect-no-replay", () => ActiveReaderNoReplayAsync(options));
    return;
case "active-reader-second-command-in-progress":
    await RunScenarioAsync("active-reader-second-command-in-progress", () => ActiveReaderSecondCommandInProgressAsync(options));
    return;
case "timeout-no-replay":
    await RunScenarioAsync("timeout-no-replay", () => CommandTimeoutNoReplayAsync(options));
    return;
case "matrix":
    await RunMatrixAsync(options);
    return;
default:
    Console.Error.WriteLine($"Unknown mode: {options.Mode}");
    Environment.ExitCode = 1;
    return;
}

static void PrintScenarioList()
{
    Console.WriteLine("open-failover");
    Console.WriteLine("admin-shutdown-replay");
    Console.WriteLine("proxy-disconnect-no-replay");
    Console.WriteLine("explicit-tx-admin-shutdown-no-replay");
    Console.WriteLine("active-reader-disconnect-no-replay");
    Console.WriteLine("active-reader-second-command-in-progress");
    Console.WriteLine("timeout-no-replay");
    Console.WriteLine("matrix");
}

static async Task RunMatrixAsync(Options options)
{
    var scenarios = new (string Name, Func<Task> Run)[]
    {
        ("open-failover", () => OpenFailoverAsync(options)),
        ("admin-shutdown-replay", () => AdminShutdownReplayAsync(options)),
        ("proxy-disconnect-no-replay", () => ProxyDisconnectNoReplayAsync(options)),
        ("explicit-tx-admin-shutdown-no-replay", () => ExplicitTransactionNoReplayAsync(options)),
        ("active-reader-disconnect-no-replay", () => ActiveReaderNoReplayAsync(options)),
        ("active-reader-second-command-in-progress", () => ActiveReaderSecondCommandInProgressAsync(options)),
        ("timeout-no-replay", () => CommandTimeoutNoReplayAsync(options))
    };

    var results = new List<(string Name, bool Passed, string Detail)>(scenarios.Length);
    foreach (var scenario in scenarios)
    {
        try
        {
            await RunScenarioAsync(scenario.Name, scenario.Run);
            results.Add((scenario.Name, true, "PASS"));
        }
        catch (Exception ex)
        {
            results.Add((scenario.Name, false, ex.GetType().Name + ": " + ex.Message));
        }

        Console.WriteLine();
    }

    Console.WriteLine("Summary");
    foreach (var result in results)
        Console.WriteLine($"{result.Name} => {(result.Passed ? "PASS" : "FAIL")} {result.Detail}");

    if (results.Any(static result => !result.Passed))
        Environment.ExitCode = 1;
}

static async Task RunScenarioAsync(string name, Func<Task> scenario)
{
    Console.WriteLine($"=== {name} ===");
    await scenario();
    Console.WriteLine("PASS");
}

static async Task OpenFailoverAsync(Options options)
{
    await using var proxyGroup = new ProxyGroup(options.Targets);
    var disabled = proxyGroup.GetByIndex(0);
    await disabled.DisableAsync();

    var connectionString = proxyGroup.ConnectionString(options.BaseExtra, "PriorityServers=2;AutoReconnect=true;MaxReconnects=3");
    Console.WriteLine($"ConnectionString={connectionString}");

    await using var conn = new GaussDBConnection(connectionString);
    await conn.OpenAsync();

    var server = await ExecuteScalarTextAsync(conn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
    Console.WriteLine($"connected-via={conn.Host}:{conn.Port} server={server}");

    if (conn.Port == disabled.Port)
        throw new InvalidOperationException("Open fallback did not skip the disabled primary seed.");
}

static async Task AdminShutdownReplayAsync(Options options)
{
    var connectionString = ConnectionStringUtil.BuildConnectionString(options.Targets, options.BaseExtra, "PriorityServers=2;AutoReconnect=true;MaxReconnects=3");
    Console.WriteLine($"ConnectionString={connectionString}");

    await using var conn = new GaussDBConnection(connectionString);
    await conn.OpenAsync();

    var initialHost = conn.Host;
    var initialPid = await ExecuteScalarLongAsync(conn, "SELECT pg_backend_pid();");
    Console.WriteLine($"initial-host={initialHost} initial-pid={initialPid}");

    var queryTask = ExecuteScalarTextAsync(
        conn,
        "SELECT inet_server_addr()::text || ':' || inet_server_port()::text || '|pid=' || pg_backend_pid()::text FROM pg_sleep(8);");

    await Task.Delay(options.FailDelay);
    await TerminateBackendAsync(ConnectionStringUtil.BuildConnectionString(new[] { initialHost! }, options.BaseExtra, string.Empty), initialPid);
    Console.WriteLine($"terminated-pid={initialPid}");

    var replayResult = await queryTask;
    var currentPid = await ExecuteScalarLongAsync(conn, "SELECT pg_backend_pid();");
    Console.WriteLine($"replay-result={replayResult}");
    Console.WriteLine($"post-reconnect host={conn.Host} pid={currentPid}");

    if (currentPid == initialPid)
        throw new InvalidOperationException("Command replay did not establish a new backend.");

    if (replayResult.Contains($"pid={initialPid}", StringComparison.Ordinal))
        throw new InvalidOperationException("Query result still came from the terminated backend.");
}

static async Task ProxyDisconnectNoReplayAsync(Options options)
{
    await using var proxyGroup = new ProxyGroup(options.Targets);
    var connectionString = proxyGroup.ConnectionString(options.BaseExtra, "PriorityServers=2;AutoReconnect=true;MaxReconnects=3");
    Console.WriteLine($"ConnectionString={connectionString}");

    await using var conn = new GaussDBConnection(connectionString);
    await conn.OpenAsync();

    var currentProxy = proxyGroup.FindByPort(conn.Port)
                      ?? throw new InvalidOperationException($"No proxy found for port {conn.Port}");
    Console.WriteLine($"initial-proxy={currentProxy.Endpoint} target={currentProxy.Target}");

    var queryTask = ExecuteScalarTextAsync(
        conn,
        "SELECT inet_server_addr()::text || ':' || inet_server_port()::text FROM pg_sleep(8);");

    await Task.Delay(options.FailDelay);
    await currentProxy.DisableAsync();
    Console.WriteLine($"disabled-proxy={currentProxy.Endpoint}");

    Exception? captured = null;
    try
    {
        var result = await queryTask;
        throw new InvalidOperationException($"Command unexpectedly succeeded with result {result}");
    }
    catch (Exception ex)
    {
        captured = ex;
    }

    if (captured is InvalidOperationException invalidOperationException &&
        invalidOperationException.Message.StartsWith("Command unexpectedly succeeded", StringComparison.Ordinal))
        throw captured;

    Console.WriteLine($"captured={captured!.GetType().Name}: {captured.Message}");
}

static async Task ExplicitTransactionNoReplayAsync(Options options)
{
    var connectionString = ConnectionStringUtil.BuildConnectionString(options.Targets, options.BaseExtra, "PriorityServers=2;AutoReconnect=true;MaxReconnects=3");
    Console.WriteLine($"ConnectionString={connectionString}");

    await using var conn = new GaussDBConnection(connectionString);
    await conn.OpenAsync();
    await using var tx = await conn.BeginTransactionAsync();

    var initialHost = conn.Host;
    var initialPid = await ExecuteScalarLongAsync(conn, "SELECT pg_backend_pid();", tx);
    Console.WriteLine($"initial-host={initialHost} initial-pid={initialPid}");

    var queryTask = ExecuteScalarTextAsync(
        conn,
        "SELECT inet_server_addr()::text || ':' || inet_server_port()::text || '|pid=' || pg_backend_pid()::text FROM pg_sleep(8);",
        tx);

    await Task.Delay(options.FailDelay);
    await TerminateBackendAsync(ConnectionStringUtil.BuildConnectionString(new[] { initialHost! }, options.BaseExtra, string.Empty), initialPid);
    Console.WriteLine($"terminated-pid={initialPid}");

    Exception? captured = null;
    try
    {
        var result = await queryTask;
        throw new InvalidOperationException($"Explicit transaction command unexpectedly succeeded with result {result}");
    }
    catch (Exception ex)
    {
        captured = ex;
    }

    if (captured is InvalidOperationException invalidOperationException &&
        invalidOperationException.Message.StartsWith("Explicit transaction command unexpectedly succeeded", StringComparison.Ordinal))
        throw captured;

    Console.WriteLine($"captured={captured!.GetType().Name}: {captured.Message}");
}

static async Task ActiveReaderNoReplayAsync(Options options)
{
    await using var proxyGroup = new ProxyGroup(options.Targets);
    var connectionString = proxyGroup.ConnectionString(options.BaseExtra, "PriorityServers=2;AutoReconnect=true;MaxReconnects=3");
    Console.WriteLine($"ConnectionString={connectionString}");

    await using var conn = new GaussDBConnection(connectionString);
    await conn.OpenAsync();
    var currentProxy = proxyGroup.FindByPort(conn.Port)
                      ?? throw new InvalidOperationException($"No proxy found for port {conn.Port}");

    await using var cmd = conn.CreateCommand();
    cmd.CommandText = "SELECT repeat('x', 50000000);";
    GaussDBDataReader? reader = null;
    TextReader? textReader = null;
    Exception? captured = null;
    try
    {
        reader = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess);
        if (!await reader.ReadAsync())
            throw new InvalidOperationException("Reader did not return the large text row.");

        textReader = reader.GetTextReader(0);
        var buffer = new char[8192];
        var firstRead = await textReader.ReadAsync(buffer, 0, buffer.Length);
        Console.WriteLine($"reader-first-chunk={firstRead} proxy={currentProxy.Endpoint} target={currentProxy.Target}");
        if (firstRead <= 0)
            throw new InvalidOperationException("Reader did not return the first text chunk.");

        await currentProxy.DisableAsync();
        Console.WriteLine($"disabled-proxy={currentProxy.Endpoint}");

        while (true)
        {
            var read = await textReader.ReadAsync(buffer, 0, buffer.Length);
            if (read == 0)
                break;
        }

        throw new InvalidOperationException("Reader unexpectedly completed after proxy disconnect.");
    }
    catch (Exception ex)
    {
        captured = ex;
    }
    finally
    {
        textReader?.Dispose();
        if (reader is not null)
        {
            try
            {
                await reader.DisposeAsync();
            }
            catch (Exception ex) when (captured is not null)
            {
                Console.WriteLine($"dispose-captured={ex.GetType().Name}: {ex.Message}");
                if (captured is InvalidOperationException pendingInvalidOperation &&
                    pendingInvalidOperation.Message.StartsWith("Reader unexpectedly completed", StringComparison.Ordinal))
                    captured = ex;
            }
        }
    }

    if (captured is InvalidOperationException invalidOperationException &&
        invalidOperationException.Message.StartsWith("Reader unexpectedly completed", StringComparison.Ordinal))
        throw captured;

    Console.WriteLine($"captured={captured!.GetType().Name}: {captured.Message}");
}

static async Task ActiveReaderSecondCommandInProgressAsync(Options options)
{
    var connectionString = ConnectionStringUtil.BuildConnectionString(options.Targets, options.BaseExtra, "PriorityServers=2;AutoReconnect=true;MaxReconnects=3");
    Console.WriteLine($"ConnectionString={connectionString}");

    await using var conn = new GaussDBConnection(connectionString);
    await conn.OpenAsync();

    await using (var activeReaderCommand = conn.CreateCommand())
    {
        activeReaderCommand.CommandText = """
SELECT i, repeat('x', 8192)
FROM generate_series(1, 100000) AS s(i);
""";

        await using var activeReader = await activeReaderCommand.ExecuteReaderAsync(CommandBehavior.SequentialAccess);
        if (!await activeReader.ReadAsync())
            throw new InvalidOperationException("Active reader scenario did not return the first row.");

        var firstRow = activeReader.GetInt32(0);
        using var textReader = activeReader.GetTextReader(1);
        var buffer = new char[256];
        var firstRead = await textReader.ReadAsync(buffer, 0, buffer.Length);
        Console.WriteLine($"active-reader-first-row={firstRow} first-read={firstRead}");
        if (firstRow != 1 || firstRead <= 0)
            throw new InvalidOperationException("Active reader scenario did not keep the first reader alive as expected.");

        await using var secondCommand = conn.CreateCommand();
        secondCommand.CommandText = "SELECT 1;";

        Exception? captured = null;
        try
        {
            await secondCommand.ExecuteReaderAsync();
            throw new InvalidOperationException("Second command unexpectedly succeeded while the first reader was still active.");
        }
        catch (Exception ex)
        {
            captured = ex;
        }

        if (captured is InvalidOperationException invalidOperationException &&
            invalidOperationException.Message.StartsWith("Second command unexpectedly succeeded", StringComparison.Ordinal))
            throw captured;

        if (captured is not GaussDBOperationInProgressException)
            throw new InvalidOperationException(
                $"Expected {nameof(GaussDBOperationInProgressException)} but captured {captured!.GetType().Name}: {captured.Message}",
                captured);

        Console.WriteLine($"captured={captured.GetType().Name}: {captured.Message}");
    }

    var postCheck = await ExecuteScalarLongAsync(conn, "SELECT 1;");
    Console.WriteLine($"post-check={postCheck}");
    if (postCheck != 1)
        throw new InvalidOperationException($"Unexpected post-check result {postCheck}.");
}

static async Task CommandTimeoutNoReplayAsync(Options options)
{
    var connectionString = ConnectionStringUtil.BuildConnectionString(options.Targets, options.BaseExtra, "PriorityServers=2;AutoReconnect=true;MaxReconnects=3;Command Timeout=1");
    Console.WriteLine($"ConnectionString={connectionString}");

    await using var conn = new GaussDBConnection(connectionString);
    await conn.OpenAsync();

    var initialPid = await ExecuteScalarLongAsync(conn, "SELECT pg_backend_pid();");
    Console.WriteLine($"initial-pid={initialPid}");

    Exception? captured = null;
    try
    {
        var result = await ExecuteScalarTextAsync(conn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text FROM pg_sleep(5);");
        throw new InvalidOperationException($"Timed out command unexpectedly succeeded with result {result}");
    }
    catch (Exception ex)
    {
        captured = ex;
    }

    if (captured is InvalidOperationException invalidOperationException &&
        invalidOperationException.Message.StartsWith("Timed out command unexpectedly succeeded", StringComparison.Ordinal))
        throw captured;

    var afterPid = await ExecuteScalarLongAsync(conn, "SELECT pg_backend_pid();");
    Console.WriteLine($"captured={captured!.GetType().Name}: {captured.Message}");
    Console.WriteLine($"after-timeout pid={afterPid}");

    if (afterPid != initialPid)
        throw new InvalidOperationException("Timeout scenario reconnected to a new backend unexpectedly.");
}

static async Task<long> ExecuteScalarLongAsync(GaussDBConnection conn, string sql, GaussDBTransaction? tx = null)
{
    var value = await ExecuteScalarAsync(conn, sql, tx);
    return Convert.ToInt64(value);
}

static async Task<string> ExecuteScalarTextAsync(GaussDBConnection conn, string sql, GaussDBTransaction? tx = null)
{
    var value = await ExecuteScalarAsync(conn, sql, tx);
    return Convert.ToString(value) ?? "<null>";
}

static async Task<object?> ExecuteScalarAsync(GaussDBConnection conn, string sql, GaussDBTransaction? tx = null)
{
    await using var cmd = conn.CreateCommand();
    cmd.CommandText = sql;
    if (tx is not null)
        cmd.Transaction = tx;
    return await cmd.ExecuteScalarAsync();
}

static async Task TerminateBackendAsync(string controlConnectionString, long pid)
{
    await using var control = new GaussDBConnection(controlConnectionString);
    await control.OpenAsync();
    await using var cmd = control.CreateCommand();
    cmd.CommandText = $"SELECT pg_terminate_backend({pid});";
    var terminated = Convert.ToBoolean(await cmd.ExecuteScalarAsync());
    if (!terminated)
        throw new InvalidOperationException($"pg_terminate_backend({pid}) returned false.");
}

sealed class ConnectionStringUtil
{
    internal static string BuildConnectionString(IReadOnlyList<string> targets, string baseExtra, string scenarioExtra)
    {
        var hostPart = string.Join(',', targets);
        if (string.IsNullOrWhiteSpace(scenarioExtra))
            return $"Host={hostPart};{baseExtra}";

        return $"Host={hostPart};{baseExtra};{scenarioExtra}";
    }
}

sealed record Options(
    string Mode,
    string[] Targets,
    string BaseExtra,
    TimeSpan FailDelay)
{
    internal static Options Parse(string[] args)
    {
        var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            if (!arg.StartsWith("--", StringComparison.Ordinal))
                continue;

            var key = arg[2..];
            if (i + 1 >= args.Length || args[i + 1].StartsWith("--", StringComparison.Ordinal))
                throw new ArgumentException($"Missing value for option --{key}");

            values[key] = args[++i];
        }

        var mode = args.FirstOrDefault(arg => !arg.StartsWith("--", StringComparison.Ordinal))?.ToLowerInvariant()
                   ?? GetValue(values, "mode", "REAL_GAUSS_SCENARIO_MODE", "matrix");
        var targets = GetValue(values, "targets", "REAL_GAUSS_TARGETS",
                "60.204.173.73:8000,113.44.50.25:8000,124.70.197.117:8000")
            .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        var baseExtra = GetValue(values, "extra", "REAL_GAUSS_EXTRA",
            "Database=postgres;Username=root;Password=Gauss_234net,;Timeout=5;Command Timeout=30;SSL Mode=Disable;Pooling=false;Multiplexing=false;UsingEip=true");
        var failDelayMs = int.Parse(GetValue(values, "fail-delay-ms", "REAL_GAUSS_FAIL_DELAY_MS", "1000"));

        return new(mode, targets, baseExtra, TimeSpan.FromMilliseconds(failDelayMs));
    }

    static string GetValue(IReadOnlyDictionary<string, string> values, string key, string envVar, string defaultValue)
        => values.TryGetValue(key, out var value)
            ? value
            : Environment.GetEnvironmentVariable(envVar) ?? defaultValue;
}

sealed class ProxyGroup : IAsyncDisposable
{
    readonly RealTcpFaultProxy[] _proxies;

    internal ProxyGroup(IReadOnlyList<string> targets)
        => _proxies = targets.Select(ParseEndpoint).Select(endpoint => RealTcpFaultProxy.Start(endpoint.Host, endpoint.Port)).ToArray();

    internal RealTcpFaultProxy GetByIndex(int index)
    {
        if (index < 0 || index >= _proxies.Length)
            throw new ArgumentOutOfRangeException(nameof(index));

        return _proxies[index];
    }

    internal RealTcpFaultProxy? FindByPort(int port)
        => _proxies.FirstOrDefault(proxy => proxy.Port == port);

    internal string ConnectionString(string baseExtra, string scenarioExtra)
        => ConnectionStringUtil.BuildConnectionString(_proxies.Select(static proxy => proxy.Endpoint).ToArray(), baseExtra, scenarioExtra);

    public async ValueTask DisposeAsync()
    {
        foreach (var proxy in _proxies)
            await proxy.DisposeAsync();
    }

    static (string Host, int Port) ParseEndpoint(string value)
    {
        var parts = value.Split(':', 2, StringSplitOptions.TrimEntries);
        if (parts.Length != 2 || !int.TryParse(parts[1], out var port))
            throw new ArgumentException($"Invalid endpoint: {value}");
        return (parts[0], port);
    }
}

sealed class RealTcpFaultProxy : IAsyncDisposable
{
    readonly TcpListener _listener;
    readonly CancellationTokenSource _shutdownCts = new();
    readonly ConcurrentDictionary<int, ConnectionPair> _connections = new();
    readonly Task _acceptLoopTask;
    readonly string _targetHost;
    readonly int _targetPort;
    int _nextConnectionId;
    bool _disabled;
    volatile bool _rejectNewConnections;

    internal string Endpoint => $"{IPAddress.Loopback}:{Port}";
    internal string Target => $"{_targetHost}:{_targetPort}";
    internal int Port { get; }

    RealTcpFaultProxy(string targetHost, int targetPort)
    {
        _targetHost = targetHost;
        _targetPort = targetPort;
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _acceptLoopTask = RunAcceptLoopAsync();
    }

    internal static RealTcpFaultProxy Start(string targetHost, int targetPort)
        => new(targetHost, targetPort);

    internal async Task DisableAsync()
    {
        if (_disabled)
            return;

        _disabled = true;
        _rejectNewConnections = true;
        _shutdownCts.Cancel();
        _listener.Stop();
        DisconnectExistingConnections();

        try
        {
            await _acceptLoopTask.ConfigureAwait(false);
        }
        catch
        {
        }
    }

    void DisconnectExistingConnections()
    {
        foreach (var connection in _connections.Values)
            connection.Close();
    }

    async Task RunAcceptLoopAsync()
    {
        while (!_shutdownCts.IsCancellationRequested)
        {
            TcpClient client;
            try
            {
                client = await _listener.AcceptTcpClientAsync(_shutdownCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (_shutdownCts.IsCancellationRequested)
            {
                break;
            }
            catch (ObjectDisposedException) when (_shutdownCts.IsCancellationRequested || _disabled)
            {
                break;
            }
            catch (SocketException) when (_shutdownCts.IsCancellationRequested || _disabled)
            {
                break;
            }

            if (_rejectNewConnections)
            {
                Abort(client);
                continue;
            }

            _ = HandleClientAsync(client);
        }
    }

    async Task HandleClientAsync(TcpClient client)
    {
        TcpClient? server = null;
        var connectionId = Interlocked.Increment(ref _nextConnectionId);

        try
        {
            server = new TcpClient();
            await server.ConnectAsync(_targetHost, _targetPort, _shutdownCts.Token).ConfigureAwait(false);

            var pair = new ConnectionPair(client, server);
            _connections[connectionId] = pair;
            await pair.RunAsync(_shutdownCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_shutdownCts.IsCancellationRequested)
        {
        }
        catch
        {
            client.Dispose();
            server?.Dispose();
        }
        finally
        {
            _connections.TryRemove(connectionId, out _);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await DisableAsync().ConfigureAwait(false);
        _shutdownCts.Dispose();
    }

    static void Abort(TcpClient client)
    {
        try
        {
            if (client.Client is { } socket)
                socket.LingerState = new LingerOption(true, 0);
        }
        catch
        {
        }

        try
        {
            client.Close();
        }
        catch
        {
        }
    }

    sealed class ConnectionPair(TcpClient client, TcpClient server)
    {
        readonly TcpClient _client = client;
        readonly TcpClient _server = server;

        internal async Task RunAsync(CancellationToken cancellationToken)
        {
            using (_client)
            using (_server)
            {
                var clientStream = _client.GetStream();
                var serverStream = _server.GetStream();

                var clientToServer = PumpAsync(clientStream, serverStream, cancellationToken);
                var serverToClient = PumpAsync(serverStream, clientStream, cancellationToken);

                await Task.WhenAny(clientToServer, serverToClient).ConfigureAwait(false);
                Close();

                try
                {
                    await Task.WhenAll(clientToServer, serverToClient).ConfigureAwait(false);
                }
                catch
                {
                }
            }
        }

        internal void Close()
        {
            Abort(_client);
            Abort(_server);
        }

        static async Task PumpAsync(NetworkStream source, NetworkStream destination, CancellationToken cancellationToken)
        {
            await source.CopyToAsync(destination, 81920, cancellationToken).ConfigureAwait(false);
            await destination.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
