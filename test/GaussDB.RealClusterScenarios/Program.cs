using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Data;
using System.Net;
using System.Net.Sockets;
using System.Text;
using HuaweiCloud.GaussDB;

var options = Options.Parse(args);

// 通过命令行 mode 选择要执行的场景，便于单独复现某一种重连或故障转移行为。
switch (options.Mode)
{
case "list":
    PrintScenarioList();
    return;
case "open-failover":
    await RunScenarioAsync("open-failover", () => OpenFailoverAsync(options));
    return;
case "inspect-routing":
    await RunScenarioAsync("inspect-routing", () => InspectRoutingAsync(options));
    return;
case "cn-discovery-roundrobin":
    await RunScenarioAsync("cn-discovery-roundrobin", () => CnDiscoveryRoundRobinAsync(options));
    return;
case "cn-discovery-proxy-seed-binding":
    await RunScenarioAsync("cn-discovery-proxy-seed-binding", () => CnDiscoveryProxySeedBindingAsync(options));
    return;
case "cn-discovery-forged-expanded-node-failover":
    await RunScenarioAsync("cn-discovery-forged-expanded-node-failover", () => CnDiscoveryForgedExpandedNodeFailoverAsync(options));
    return;
case "cn-discovery-forged-reachable-proxy-seed-binding":
    await RunScenarioAsync("cn-discovery-forged-reachable-proxy-seed-binding", () => CnDiscoveryForgedReachableProxySeedBindingAsync(options));
    return;
case "cn-discovery-misconfigured-priority-seed-pollutes-cluster":
    await RunScenarioAsync("cn-discovery-misconfigured-priority-seed-pollutes-cluster", () => CnDiscoveryMisconfiguredPrioritySeedPollutesClusterAsync(options));
    return;
case "cn-discovery-unbound-fallback-seed-allows-foreign-node-adoption":
    await RunScenarioAsync("cn-discovery-unbound-fallback-seed-allows-foreign-node-adoption", () => CnDiscoveryUnboundFallbackSeedAllowsForeignNodeAdoptionAsync(options));
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
    Console.WriteLine("inspect-routing");
    Console.WriteLine("cn-discovery-roundrobin");
    Console.WriteLine("cn-discovery-proxy-seed-binding");
    Console.WriteLine("cn-discovery-forged-expanded-node-failover");
    Console.WriteLine("cn-discovery-forged-reachable-proxy-seed-binding");
    Console.WriteLine("cn-discovery-misconfigured-priority-seed-pollutes-cluster");
    Console.WriteLine("cn-discovery-unbound-fallback-seed-allows-foreign-node-adoption");
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
    // matrix 模式会把所有场景串起来跑一遍，并把每个场景的结果单独汇总。
    var scenarios = new (string Name, Func<Task> Run)[]
    {
        ("open-failover", () => OpenFailoverAsync(options)),
        ("cn-discovery-roundrobin", () => CnDiscoveryRoundRobinAsync(options)),
        ("cn-discovery-proxy-seed-binding", () => CnDiscoveryProxySeedBindingAsync(options)),
        ("cn-discovery-forged-expanded-node-failover", () => CnDiscoveryForgedExpandedNodeFailoverAsync(options)),
        ("cn-discovery-forged-reachable-proxy-seed-binding", () => CnDiscoveryForgedReachableProxySeedBindingAsync(options)),
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
    // 统一打印场景标题和 PASS 标记，方便在命令行里快速定位失败点。
    Console.WriteLine($"=== {name} ===");
    await scenario();
    Console.WriteLine("PASS");
}

static async Task InspectRoutingAsync(Options options)
{
    var connectionString = ConnectionStringUtil.BuildConnectionString(options.Targets, options.BaseExtra, string.Empty);
    var builder = new GaussDBConnectionStringBuilder(connectionString);
    Console.WriteLine($"ConnectionString={connectionString}");
    Console.WriteLine($"seed-targets={string.Join(",", options.Targets)}");
    Console.WriteLine($"using-eip={builder.UsingEip}");

    await using var dataSource = new GaussDBDataSourceBuilder(builder.ConnectionString).BuildMultiHost();
    await using var conn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);

    var currentNodeName = await ExecuteScalarTextAsync(conn, "SELECT get_nodename();");
    var serverEndpoint = await ExecuteScalarTextAsync(conn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
    Console.WriteLine($"connected-via={conn.Host}:{conn.Port} server={serverEndpoint} node-name={currentNodeName}");

    var coordinators = await LoadActiveCoordinatorsAsync(conn);
    if (coordinators.Count == 0)
        throw new InvalidOperationException("pgxc_node returned no active CN rows.");

    var currentNodeFound = false;
    foreach (var coordinator in coordinators)
    {
        var preferredEndpoint = coordinator.GetPreferredEndpoint(builder.UsingEip);
        Console.WriteLine(
            $"pgxc-node node_name={coordinator.NodeName} " +
            $"host={coordinator.HostEndpoint} eip={coordinator.EipEndpoint} preferred={preferredEndpoint}");
        currentNodeFound |= coordinator.NodeName == currentNodeName;
    }

    if (!currentNodeFound)
        throw new InvalidOperationException($"Current CN node_name '{currentNodeName}' was not present in active pgxc_node rows.");
}

static async Task CnDiscoveryRoundRobinAsync(Options options)
{
    var seedTarget = options.Targets[0];
    var seedConnectionString = ConnectionStringUtil.BuildConnectionString(new[] { seedTarget }, options.BaseExtra, string.Empty);
    var seedBuilder = new GaussDBConnectionStringBuilder(seedConnectionString);

    Console.WriteLine($"seed-target={seedTarget}");
    Console.WriteLine($"using-eip={seedBuilder.UsingEip}");

    await using var seedConn = new GaussDBConnection(seedConnectionString);
    await seedConn.OpenAsync();

    var seedNodeName = await ExecuteScalarTextAsync(seedConn, "SELECT get_nodename();");
    var coordinators = await LoadActiveCoordinatorsAsync(seedConn);
    if (coordinators.Count < 2)
        throw new InvalidOperationException("CN discovery scenario requires at least two active coordinators in pgxc_node.");

    var expectedNodes = coordinators
        .Select(static coordinator => coordinator.NodeName)
        .OrderBy(static x => x, StringComparer.Ordinal)
        .ToArray();
    var expectedPreferredEndpoints = coordinators
        .Select(coordinator => coordinator.GetPreferredEndpoint(seedBuilder.UsingEip))
        .OrderBy(static endpoint => endpoint.ToString(), StringComparer.Ordinal)
        .ToArray();
    var seedEndpoint = ParseEndpoint(seedTarget);
    var directlyReachableNodes = new HashSet<string>(StringComparer.Ordinal);
    foreach (var coordinator in coordinators)
    {
        if (await CanConnectAsync(coordinator.GetPreferredEndpoint(seedBuilder.UsingEip)))
            directlyReachableNodes.Add(coordinator.NodeName);
    }
    var directlyReachableDiscoveredNodes = coordinators
        .Where(coordinator => coordinator.NodeName != seedNodeName && directlyReachableNodes.Contains(coordinator.NodeName))
        .Select(static coordinator => coordinator.NodeName)
        .OrderBy(static x => x, StringComparer.Ordinal)
        .ToArray();

    Console.WriteLine($"seed-node-name={seedNodeName}");
    Console.WriteLine($"expected-nodes={string.Join(",", expectedNodes)}");
    Console.WriteLine($"expected-preferred-endpoints={string.Join(",", expectedPreferredEndpoints)}");
    Console.WriteLine($"directly-reachable-nodes={string.Join(",", directlyReachableNodes.OrderBy(static x => x, StringComparer.Ordinal))}");
    Console.WriteLine($"directly-reachable-discovered-nodes={string.Join(",", directlyReachableDiscoveredNodes)}");

    var discoveryConnectionString = ConnectionStringUtil.BuildConnectionString(
        new[] { seedTarget },
        options.BaseExtra,
        "AutoBalance=roundrobin;RefreshCNIpListTime=30");
    Console.WriteLine($"ConnectionString={discoveryConnectionString}");

    await using var dataSource = new GaussDBDataSourceBuilder(discoveryConnectionString).BuildMultiHost();
    var observedNodes = new HashSet<string>(StringComparer.Ordinal);
    var observedEndpoints = new HashSet<string>(StringComparer.Ordinal);
    var maxAttempts = coordinators.Count * 2;
    for (var i = 0; i < maxAttempts && observedNodes.Count < expectedNodes.Length; i++)
    {
        await using var conn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);
        var nodeName = await ExecuteScalarTextAsync(conn, "SELECT get_nodename();");
        var serverEndpoint = await ExecuteScalarTextAsync(conn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
        var connectedEndpoint = new Endpoint(conn.Host!, conn.Port);

        observedNodes.Add(nodeName);
        observedEndpoints.Add(connectedEndpoint.ToString());
        Console.WriteLine($"open[{i + 1}] connected-via={connectedEndpoint} server={serverEndpoint} node-name={nodeName}");
    }

    var observedNodesOrdered = observedNodes.OrderBy(static x => x, StringComparer.Ordinal).ToArray();
    Console.WriteLine($"observed-nodes={string.Join(",", observedNodesOrdered)}");
    Console.WriteLine($"observed-endpoints={string.Join(",", observedEndpoints.OrderBy(static x => x, StringComparer.Ordinal))}");

    if (directlyReachableDiscoveredNodes.Length == 0)
    {
        if (observedNodesOrdered.Length != 1 || observedNodesOrdered[0] != seedNodeName)
            throw new InvalidOperationException(
                $"No discovered non-seed endpoint was directly reachable, so the driver should have fallen back to the seed node only. observed=[{string.Join(",", observedNodesOrdered)}]");

        Console.WriteLine("validation-mode=seed-fallback");
        return;
    }

    if (!directlyReachableDiscoveredNodes.All(node => observedNodes.Contains(node)))
        throw new InvalidOperationException(
            $"Observed node set did not include every directly reachable discovered node. expected-reachable=[{string.Join(",", directlyReachableDiscoveredNodes)}] observed=[{string.Join(",", observedNodesOrdered)}]");

    var discoveredPreferredEndpoints = expectedPreferredEndpoints
        .Where(endpoint => endpoint.ToString() != seedEndpoint.ToString())
        .ToArray();
    if (discoveredPreferredEndpoints.Length == 0)
        throw new InvalidOperationException("pgxc_node did not expose any preferred endpoint beyond the single seed host.");

    if (!discoveredPreferredEndpoints.Any(endpoint => observedEndpoints.Contains(endpoint.ToString())))
        throw new InvalidOperationException(
            $"Observed endpoints did not include any discovered preferred endpoint. observed=[{string.Join(",", observedEndpoints.OrderBy(static x => x, StringComparer.Ordinal))}]");

    Console.WriteLine("validation-mode=direct-discovery");
}

static async Task CnDiscoveryProxySeedBindingAsync(Options options)
{
    var seedTarget = options.Targets[0];
    var seedConnectionString = ConnectionStringUtil.BuildConnectionString(new[] { seedTarget }, options.BaseExtra, string.Empty);
    var seedBuilder = new GaussDBConnectionStringBuilder(seedConnectionString);

    await using var seedConn = new GaussDBConnection(seedConnectionString);
    await seedConn.OpenAsync();

    var seedNodeName = await ExecuteScalarTextAsync(seedConn, "SELECT get_nodename();");
    var coordinators = await LoadActiveCoordinatorsAsync(seedConn);
    var seedCoordinator = coordinators.FirstOrDefault(coordinator => coordinator.NodeName == seedNodeName);
    if (seedCoordinator is null)
        throw new InvalidOperationException($"Could not find metadata row for seed node '{seedNodeName}'.");

    var expectedPreferredEndpoint = seedCoordinator.GetPreferredEndpoint(seedBuilder.UsingEip);

    await using var proxyGroup = new ProxyGroup(new[] { seedTarget });
    var proxiedSeed = proxyGroup.GetByIndex(0);
    var discoveryConnectionString = ConnectionStringUtil.BuildConnectionString(
        new[] { proxiedSeed.Endpoint },
        options.BaseExtra,
        "AutoBalance=roundrobin;RefreshCNIpListTime=30");

    Console.WriteLine($"seed-target={seedTarget}");
    Console.WriteLine($"proxy-seed={proxiedSeed.Endpoint} target={proxiedSeed.Target}");
    Console.WriteLine($"seed-node-name={seedNodeName}");
    Console.WriteLine($"expected-preferred-endpoint={expectedPreferredEndpoint}");
    var preferredEndpointReachable = await CanConnectAsync(expectedPreferredEndpoint);
    Console.WriteLine($"preferred-endpoint-directly-reachable={preferredEndpointReachable}");
    Console.WriteLine($"ConnectionString={discoveryConnectionString}");

    await using var dataSource = new GaussDBDataSourceBuilder(discoveryConnectionString).BuildMultiHost();
    await using var conn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);

    var connectedNodeName = await ExecuteScalarTextAsync(conn, "SELECT get_nodename();");
    var serverEndpoint = await ExecuteScalarTextAsync(conn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
    var connectedEndpoint = new Endpoint(conn.Host!, conn.Port);
    Console.WriteLine($"connected-via={connectedEndpoint} server={serverEndpoint} node-name={connectedNodeName}");

    if (connectedNodeName != seedNodeName)
        throw new InvalidOperationException(
            $"Expected the proxied seed to bind back to node '{seedNodeName}', but connected to '{connectedNodeName}'.");

    if (preferredEndpointReachable)
    {
        if (connectedEndpoint.ToString() != expectedPreferredEndpoint.ToString())
            throw new InvalidOperationException(
                $"Expected dynamic preferred endpoint {expectedPreferredEndpoint}, but connected via {connectedEndpoint}.");

        if (conn.Port == proxiedSeed.Port)
            throw new InvalidOperationException("Connection unexpectedly stayed on the proxy seed instead of switching to the discovered preferred endpoint.");

        Console.WriteLine("validation-mode=direct-discovery");
        return;
    }

    if (conn.Port != proxiedSeed.Port)
        throw new InvalidOperationException(
            $"Preferred endpoint {expectedPreferredEndpoint} was not directly reachable, so the driver should have fallen back to the proxied seed. connected-via={connectedEndpoint}");

    Console.WriteLine("validation-mode=seed-fallback");
}

static async Task CnDiscoveryForgedExpandedNodeFailoverAsync(Options options)
{
    var seedRoutes = await LoadSeedRoutesAsync(options);
    if (seedRoutes.Length < 2)
        throw new InvalidOperationException("Forged expanded node scenario requires at least two seed targets.");

    var seedRoute = seedRoutes[0];
    var expandedRoute = seedRoutes[1];
    await using var expandedProxy = RealTcpFaultProxy.Start(expandedRoute.SeedEndpoint.Host, expandedRoute.SeedEndpoint.Port);
    var expandedEndpoint = ParseEndpoint(expandedProxy.Endpoint);
    var unreachableSeedEndpoint = GetUnreachableEndpoint();
    var overrides = new[]
    {
        new CoordinatorMetadata(seedRoute.NodeName, unreachableSeedEndpoint, unreachableSeedEndpoint),
        new CoordinatorMetadata(expandedRoute.NodeName, expandedEndpoint, expandedEndpoint)
    };

    await using var metadataProxy = PgMetadataRewriteProxy.Start(
        seedRoute.SeedEndpoint.Host,
        seedRoute.SeedEndpoint.Port,
        overrides);
    var discoveryConnectionString = ConnectionStringUtil.BuildConnectionString(
        new[] { metadataProxy.Endpoint },
        options.BaseExtra,
        "AutoBalance=roundrobin;RefreshCNIpListTime=30");

    Console.WriteLine($"seed-target={seedRoute.Target}");
    Console.WriteLine($"expanded-target={expandedRoute.Target}");
    Console.WriteLine($"metadata-proxy={metadataProxy.Endpoint} target={metadataProxy.Target}");
    Console.WriteLine($"seed-node-name={seedRoute.NodeName}");
    Console.WriteLine($"expanded-node-name={expandedRoute.NodeName}");
    Console.WriteLine($"forged-expanded-endpoint={expandedEndpoint}");
    Console.WriteLine($"ConnectionString={discoveryConnectionString}");

    await using var dataSource = new GaussDBDataSourceBuilder(discoveryConnectionString).BuildMultiHost();
    await using (var warmConn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any))
    {
        var warmNodeName = await ExecuteScalarTextAsync(warmConn, "SELECT get_nodename();");
        var warmServerEndpoint = await ExecuteScalarTextAsync(warmConn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
        Console.WriteLine($"warm-open connected-via={warmConn.Host}:{warmConn.Port} server={warmServerEndpoint} node-name={warmNodeName}");
    }

    Console.WriteLine($"metadata-proxy-rewritten-rows={metadataProxy.RewrittenRowCount}");
    Console.WriteLine($"metadata-proxy-seen-sql={string.Join(" || ", metadataProxy.SeenSql)}");

    await metadataProxy.DisableAsync();
    Console.WriteLine($"disabled-metadata-proxy={metadataProxy.Endpoint}");

    await using var failoverConn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);
    var failoverNodeName = await ExecuteScalarTextAsync(failoverConn, "SELECT get_nodename();");
    var failoverServerEndpoint = await ExecuteScalarTextAsync(failoverConn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
    var failoverEndpoint = new Endpoint(failoverConn.Host!, failoverConn.Port);
    Console.WriteLine($"failover-open connected-via={failoverEndpoint} server={failoverServerEndpoint} node-name={failoverNodeName}");

    if (failoverNodeName != expandedRoute.NodeName)
        throw new InvalidOperationException(
            $"Expected failover to forged discovered node '{expandedRoute.NodeName}', but connected to '{failoverNodeName}'.");

    if (failoverEndpoint.ToString() != expandedEndpoint.ToString())
        throw new InvalidOperationException(
            $"Expected failover via forged expanded endpoint {expandedEndpoint}, but connected via {failoverEndpoint}.");

    Console.WriteLine("validation-mode=forged-expanded-node");
}

static async Task CnDiscoveryForgedReachableProxySeedBindingAsync(Options options)
{
    var seedRoutes = await LoadSeedRoutesAsync(options);
    var seedRoute = seedRoutes[0];

    await using var forgedPreferredProxy = RealTcpFaultProxy.Start(seedRoute.SeedEndpoint.Host, seedRoute.SeedEndpoint.Port);
    var forgedPreferredEndpoint = ParseEndpoint(forgedPreferredProxy.Endpoint);
    var overrideCoordinator = new CoordinatorMetadata(seedRoute.NodeName, forgedPreferredEndpoint, forgedPreferredEndpoint);

    await using var metadataProxy = PgMetadataRewriteProxy.Start(
        seedRoute.SeedEndpoint.Host,
        seedRoute.SeedEndpoint.Port,
        new[] { overrideCoordinator });
    var discoveryConnectionString = ConnectionStringUtil.BuildConnectionString(
        new[] { metadataProxy.Endpoint },
        options.BaseExtra,
        "AutoBalance=roundrobin;RefreshCNIpListTime=30");

    Console.WriteLine($"seed-target={seedRoute.Target}");
    Console.WriteLine($"seed-node-name={seedRoute.NodeName}");
    Console.WriteLine($"metadata-proxy={metadataProxy.Endpoint} target={metadataProxy.Target}");
    Console.WriteLine($"forged-preferred-endpoint={forgedPreferredEndpoint}");
    Console.WriteLine($"ConnectionString={discoveryConnectionString}");

    await using var dataSource = new GaussDBDataSourceBuilder(discoveryConnectionString).BuildMultiHost();
    await using var conn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);

    var connectedNodeName = await ExecuteScalarTextAsync(conn, "SELECT get_nodename();");
    var serverEndpoint = await ExecuteScalarTextAsync(conn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
    var connectedEndpoint = new Endpoint(conn.Host!, conn.Port);
    Console.WriteLine($"connected-via={connectedEndpoint} server={serverEndpoint} node-name={connectedNodeName}");
    Console.WriteLine($"metadata-proxy-rewritten-rows={metadataProxy.RewrittenRowCount}");
    Console.WriteLine($"metadata-proxy-seen-sql={string.Join(" || ", metadataProxy.SeenSql)}");

    if (connectedNodeName != seedRoute.NodeName)
        throw new InvalidOperationException(
            $"Expected the rewritten proxied seed to bind back to node '{seedRoute.NodeName}', but connected to '{connectedNodeName}'.");

    if (connectedEndpoint.ToString() != forgedPreferredEndpoint.ToString())
        throw new InvalidOperationException(
            $"Expected forged preferred endpoint {forgedPreferredEndpoint}, but connected via {connectedEndpoint}.");

    if (connectedEndpoint.ToString() == metadataProxy.Endpoint)
        throw new InvalidOperationException("Connection unexpectedly stayed on the metadata rewrite proxy instead of switching to the forged preferred endpoint.");

    Console.WriteLine("validation-mode=forged-direct-discovery");
}

static async Task CnDiscoveryMisconfiguredPrioritySeedPollutesClusterAsync(Options options)
{
    var seedRoutes = await LoadSeedRoutesAsync(options);
    if (seedRoutes.Length < 3)
        throw new InvalidOperationException("Misconfigured priority seed scenario requires at least three seed targets.");

    var intendedPrimarySeed = seedRoutes[0];
    var foreignClusterSeed = seedRoutes[1];
    var foreignClusterPeer = seedRoutes[2];

    await using var forgedProxyGroup = new ProxyGroup(seedRoutes.Select(static route => route.Target).ToArray());
    var forgedRoutes = seedRoutes
        .Select((route, index) => new SeedProxyRoute(route, ParseEndpoint(forgedProxyGroup.GetByIndex(index).Endpoint)))
        .ToArray();

    var misconfiguredPrioritySeed = PgMetadataRewriteProxy.Start(
        foreignClusterSeed.SeedEndpoint.Host,
        foreignClusterSeed.SeedEndpoint.Port,
        forgedRoutes.Select(static route => new CoordinatorMetadata(route.SeedRoute.NodeName, route.ProxyEndpoint, route.ProxyEndpoint)).ToArray());
    await using (misconfiguredPrioritySeed.ConfigureAwait(false))
    {
        var hostList = new[]
        {
            misconfiguredPrioritySeed.Endpoint,
            foreignClusterSeed.Target,
            foreignClusterPeer.Target
        };
        var discoveryConnectionString = ConnectionStringUtil.BuildConnectionString(
            hostList,
            options.BaseExtra,
            "PriorityServers=1;AutoBalance=roundrobin;RefreshCNIpListTime=30");

        Console.WriteLine($"intended-primary-seed={intendedPrimarySeed.Target} node={intendedPrimarySeed.NodeName}");
        Console.WriteLine($"misconfigured-priority-seed={misconfiguredPrioritySeed.Endpoint} actual-target={foreignClusterSeed.Target} actual-node={foreignClusterSeed.NodeName}");
        Console.WriteLine($"secondary-cluster-peer={foreignClusterPeer.Target} node={foreignClusterPeer.NodeName}");
        Console.WriteLine($"forged-endpoints={string.Join(",", forgedRoutes.Select(static route => route.ProxyEndpoint.ToString()))}");
        Console.WriteLine($"ConnectionString={discoveryConnectionString}");

        await using var dataSource = new GaussDBDataSourceBuilder(discoveryConnectionString).BuildMultiHost();
        await using (var warmConn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any))
        {
            var warmNodeName = await ExecuteScalarTextAsync(warmConn, "SELECT get_nodename();");
            var warmServerEndpoint = await ExecuteScalarTextAsync(warmConn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
            Console.WriteLine($"warm-open[1] connected-via={warmConn.Host}:{warmConn.Port} server={warmServerEndpoint} node-name={warmNodeName}");
        }

        await using (var discoveryConn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any))
        {
            var discoveryNodeName = await ExecuteScalarTextAsync(discoveryConn, "SELECT get_nodename();");
            var discoveryServerEndpoint = await ExecuteScalarTextAsync(discoveryConn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
            Console.WriteLine($"warm-open[2] connected-via={discoveryConn.Host}:{discoveryConn.Port} server={discoveryServerEndpoint} node-name={discoveryNodeName}");
        }

        Console.WriteLine($"metadata-proxy-rewritten-rows={misconfiguredPrioritySeed.RewrittenRowCount}");
        Console.WriteLine($"metadata-proxy-seen-sql={string.Join(" || ", misconfiguredPrioritySeed.SeenSql)}");

        if (misconfiguredPrioritySeed.RewrittenRowCount == 0)
            throw new InvalidOperationException("Expected the second open to trigger CN discovery on the misconfigured preferred cluster, but no pgxc_node rows were rewritten.");

        await misconfiguredPrioritySeed.DisableAsync();
        Console.WriteLine($"disabled-misconfigured-priority-seed={misconfiguredPrioritySeed.Endpoint}");

        await using var afterDisableConn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);
        var observedNodeName = await ExecuteScalarTextAsync(afterDisableConn, "SELECT get_nodename();");
        var observedServerEndpoint = await ExecuteScalarTextAsync(afterDisableConn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
        var observedEndpoint = new Endpoint(afterDisableConn.Host!, afterDisableConn.Port);
        Console.WriteLine($"after-disable connected-via={observedEndpoint} server={observedServerEndpoint} node-name={observedNodeName}");

        var forgedEndpointKeys = forgedRoutes.Select(static route => route.ProxyEndpoint.ToString()).ToHashSet(StringComparer.Ordinal);
        var pollutionObserved = forgedEndpointKeys.Contains(observedEndpoint.ToString());
        Console.WriteLine($"pollution-observed={pollutionObserved}");

        if (!pollutionObserved)
            throw new InvalidOperationException(
                $"Expected the misconfigured priority seed to pollute the preferred cluster and route through a forged endpoint, but connected via {observedEndpoint}.");
    }
}

static async Task CnDiscoveryUnboundFallbackSeedAllowsForeignNodeAdoptionAsync(Options options)
{
    var seedRoutes = await LoadSeedRoutesAsync(options);
    if (seedRoutes.Length < 3)
        throw new InvalidOperationException("Unbound fallback seed scenario requires at least three seed targets.");

    var primarySeed = seedRoutes[0];
    var foreignNode = seedRoutes[2];
    var unreachableFallbackSeed = GetUnreachableEndpoint();
    await using var foreignProxy = RealTcpFaultProxy.Start(foreignNode.SeedEndpoint.Host, foreignNode.SeedEndpoint.Port);
    var foreignProxyEndpoint = ParseEndpoint(foreignProxy.Endpoint);

    var primaryNodeDeadEndpoint = GetUnreachableEndpoint();
    await using var metadataProxy = PgMetadataRewriteProxy.Start(
        primarySeed.SeedEndpoint.Host,
        primarySeed.SeedEndpoint.Port,
        new[]
        {
            new CoordinatorMetadata(primarySeed.NodeName, primaryNodeDeadEndpoint, primaryNodeDeadEndpoint),
            new CoordinatorMetadata(foreignNode.NodeName, foreignProxyEndpoint, foreignProxyEndpoint)
        });

    var discoveryConnectionString = ConnectionStringUtil.BuildConnectionString(
        new[] { metadataProxy.Endpoint, unreachableFallbackSeed.ToString(), foreignNode.Target },
        options.BaseExtra,
        "PriorityServers=1;AutoBalance=roundrobin;RefreshCNIpListTime=30");

    Console.WriteLine($"primary-seed={primarySeed.Target} node={primarySeed.NodeName}");
    Console.WriteLine($"unreachable-fallback-seed={unreachableFallbackSeed}");
    Console.WriteLine($"foreign-node-seed={foreignNode.Target} node={foreignNode.NodeName}");
    Console.WriteLine($"metadata-proxy={metadataProxy.Endpoint} target={metadataProxy.Target}");
    Console.WriteLine($"forged-foreign-endpoint={foreignProxyEndpoint}");
    Console.WriteLine($"ConnectionString={discoveryConnectionString}");

    await using var dataSource = new GaussDBDataSourceBuilder(discoveryConnectionString).BuildMultiHost();
    await using (var warmConn1 = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any))
    {
        var nodeName = await ExecuteScalarTextAsync(warmConn1, "SELECT get_nodename();");
        var serverEndpoint = await ExecuteScalarTextAsync(warmConn1, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
        Console.WriteLine($"warm-open[1] connected-via={warmConn1.Host}:{warmConn1.Port} server={serverEndpoint} node-name={nodeName}");
    }

    await using (var warmConn2 = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any))
    {
        var nodeName = await ExecuteScalarTextAsync(warmConn2, "SELECT get_nodename();");
        var serverEndpoint = await ExecuteScalarTextAsync(warmConn2, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
        Console.WriteLine($"warm-open[2] connected-via={warmConn2.Host}:{warmConn2.Port} server={serverEndpoint} node-name={nodeName}");
    }

    Console.WriteLine($"metadata-proxy-rewritten-rows={metadataProxy.RewrittenRowCount}");
    Console.WriteLine($"metadata-proxy-seen-sql={string.Join(" || ", metadataProxy.SeenSql)}");

    if (metadataProxy.RewrittenRowCount == 0)
        throw new InvalidOperationException("Expected CN discovery to run on the preferred cluster, but no pgxc_node rows were rewritten.");

    await metadataProxy.DisableAsync();
    Console.WriteLine($"disabled-primary-metadata-proxy={metadataProxy.Endpoint}");

    await using var afterDisableConn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);
    var observedNodeName = await ExecuteScalarTextAsync(afterDisableConn, "SELECT get_nodename();");
    var observedServerEndpoint = await ExecuteScalarTextAsync(afterDisableConn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
    var observedEndpoint = new Endpoint(afterDisableConn.Host!, afterDisableConn.Port);
    Console.WriteLine($"after-disable connected-via={observedEndpoint} server={observedServerEndpoint} node-name={observedNodeName}");

    var pollutionObserved = observedEndpoint.ToString() == foreignProxyEndpoint.ToString();
    Console.WriteLine($"pollution-observed={pollutionObserved}");

    if (!pollutionObserved)
        throw new InvalidOperationException(
            $"Expected the preferred cluster to adopt the foreign node after the fallback seed stayed unbound, but connected via {observedEndpoint}.");
}

static async Task OpenFailoverAsync(Options options)
{
    // 验证 Open 阶段能在主节点不可达时切到备节点，而不是卡死在第一个 seed 上。
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
    // 验证命令执行中遇到 AdminShutdown 时会自动重连并重新执行安全命令。
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
    // 验证代理断开时命令不会被透明重放，调用方应收到失败而不是悄悄成功。
    await using var proxyGroup = new ProxyGroup(options.Targets);
    var connectionString = proxyGroup.ConnectionString(options.BaseExtra, "PriorityServers=2;AutoReconnect=true;MaxReconnects=3;RefreshCNIpListTime=1;AutoBalance=Shuffle");
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
    // 显式事务内不允许自动重放，否则会破坏事务语义。
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
    // 活动 reader 期间也不能自动重连重放，否则读到的流状态会失真。
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
    // 保持第一个 reader 打开时，第二个命令必须被拒绝，不能通过重连绕过去。
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
    // 超时不应触发重连或换 backend，避免把普通慢 SQL 当成故障转移。
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
    // 读标量并转成长整型，减少每个场景里的样板代码。
    var value = await ExecuteScalarAsync(conn, sql, tx);
    return Convert.ToInt64(value);
}

static async Task<string> ExecuteScalarTextAsync(GaussDBConnection conn, string sql, GaussDBTransaction? tx = null)
{
    // 读标量并转成字符串，统一处理 null。
    var value = await ExecuteScalarAsync(conn, sql, tx);
    return Convert.ToString(value) ?? "<null>";
}

static async Task<object?> ExecuteScalarAsync(GaussDBConnection conn, string sql, GaussDBTransaction? tx = null)
{
    // 公共标量执行入口，可选地附带事务对象。
    await using var cmd = conn.CreateCommand();
    cmd.CommandText = sql;
    if (tx is not null)
        cmd.Transaction = tx;
    return await cmd.ExecuteScalarAsync();
}

static async Task<List<CoordinatorMetadata>> LoadActiveCoordinatorsAsync(GaussDBConnection conn)
{
    await using var cmd = conn.CreateCommand();
    cmd.CommandText = SqlText.PgxcNodeRefresh;

    await using var reader = await cmd.ExecuteReaderAsync();
    var coordinators = new List<CoordinatorMetadata>();
    while (await reader.ReadAsync())
    {
        coordinators.Add(new(
            reader.GetString(0),
            new Endpoint(reader.GetString(1), reader.GetInt32(2)),
            new Endpoint(reader.GetString(3), reader.GetInt32(4))));
    }

    return coordinators;
}

static async Task<SeedRoute[]> LoadSeedRoutesAsync(Options options)
{
    var routes = new List<SeedRoute>(options.Targets.Length);
    for (var i = 0; i < options.Targets.Length; i++)
    {
        var target = options.Targets[i];
        var connectionString = ConnectionStringUtil.BuildConnectionString(new[] { target }, options.BaseExtra, string.Empty);
        await using var conn = new GaussDBConnection(connectionString);
        await conn.OpenAsync();
        var nodeName = await ExecuteScalarTextAsync(conn, "SELECT get_nodename();");
        routes.Add(new(i, target, ParseEndpoint(target), nodeName));
    }

    return routes.ToArray();
}

static Endpoint ParseEndpoint(string value)
{
    var parts = value.Split(':', 2, StringSplitOptions.TrimEntries);
    if (parts.Length != 2 || !int.TryParse(parts[1], out var port))
        throw new ArgumentException($"Invalid endpoint: {value}");

    return new(parts[0], port);
}

static async Task<bool> CanConnectAsync(Endpoint endpoint, int timeoutMs = 1000)
{
    using var client = new TcpClient();
    using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(timeoutMs));

    try
    {
        await client.ConnectAsync(endpoint.Host, endpoint.Port, cts.Token);
        return true;
    }
    catch
    {
        return false;
    }
}

static Endpoint GetUnreachableEndpoint()
{
    using var listener = new TcpListener(IPAddress.Loopback, 0);
    listener.Start();
    var port = ((IPEndPoint)listener.LocalEndpoint).Port;
    listener.Stop();
    return new(IPAddress.Loopback.ToString(), port);
}

static async Task TerminateBackendAsync(string controlConnectionString, long pid)
{
    // 通过控制连接显式杀掉目标 backend，用来模拟后端故障。
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
    // 把目标 endpoints 和场景专用参数拼成完整连接串，避免每个场景重复拼接。
    internal static string BuildConnectionString(IReadOnlyList<string> targets, string baseExtra, string scenarioExtra)
    {
        var hostPart = string.Join(',', targets);
        if (string.IsNullOrWhiteSpace(scenarioExtra))
            return $"Host={hostPart};{baseExtra}";

        return $"Host={hostPart};{baseExtra};{scenarioExtra}";
    }
}

sealed record CoordinatorMetadata(string NodeName, Endpoint HostEndpoint, Endpoint EipEndpoint)
{
    internal Endpoint GetPreferredEndpoint(bool usingEip)
        => usingEip ? EipEndpoint : HostEndpoint;
}

sealed record SeedRoute(int TargetIndex, string Target, Endpoint SeedEndpoint, string NodeName);

sealed record SeedProxyRoute(SeedRoute SeedRoute, Endpoint ProxyEndpoint);

readonly record struct Endpoint(string Host, int Port)
{
    public override string ToString()
        => Host.Contains(':') && !Host.StartsWith("[", StringComparison.Ordinal)
            ? $"[{Host}]:{Port}"
            : $"{Host}:{Port}";
}

static class SqlText
{
    internal const string PgxcNodeRefresh =
        "select node_name,node_host,node_port,node_host1,node_port1 " +
        "from pgxc_node where node_type='C' and nodeis_active = true order by node_name;";
}

sealed record Options(
    string Mode,
    string[] Targets,
    string BaseExtra,
    TimeSpan FailDelay)
{
    // 从命令行参数和环境变量里解析场景配置，便于本地和 CI 共用同一套入口。
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

    // 把一组真实目标包装成一组本地代理，方便统一注入断连/拒绝连接故障。
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

sealed class PgMetadataRewriteProxy : IAsyncDisposable
{
    readonly TcpListener _listener;
    readonly CancellationTokenSource _shutdownCts = new();
    readonly ConcurrentDictionary<int, RewriteConnectionPair> _connections = new();
    readonly ConcurrentQueue<string> _seenSql = new();
    readonly Task _acceptLoopTask;
    readonly string _targetHost;
    readonly int _targetPort;
    readonly IReadOnlyDictionary<string, CoordinatorMetadata> _overrides;
    int _nextConnectionId;
    bool _disabled;
    int _rewrittenRowCount;

    internal string Endpoint => $"{IPAddress.Loopback}:{Port}";
    internal string Target => $"{_targetHost}:{_targetPort}";
    internal int Port { get; }
    internal string[] SeenSql => _seenSql.ToArray();
    internal int RewrittenRowCount => _rewrittenRowCount;

    PgMetadataRewriteProxy(string targetHost, int targetPort, IReadOnlyDictionary<string, CoordinatorMetadata> overrides)
    {
        _targetHost = targetHost;
        _targetPort = targetPort;
        _overrides = overrides;
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _acceptLoopTask = RunAcceptLoopAsync();
    }

    internal static PgMetadataRewriteProxy Start(string targetHost, int targetPort, IReadOnlyList<CoordinatorMetadata> overrides)
        => new(
            targetHost,
            targetPort,
            overrides.ToDictionary(static coordinator => coordinator.NodeName, StringComparer.Ordinal));

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

            var pair = new RewriteConnectionPair(
                client,
                server,
                _overrides,
                sql => _seenSql.Enqueue(sql),
                () => Interlocked.Increment(ref _rewrittenRowCount));
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

    internal async Task DisableAsync()
    {
        if (_disabled)
            return;

        _disabled = true;
        _shutdownCts.Cancel();
        _listener.Stop();
        foreach (var connection in _connections.Values)
            connection.Close();

        try
        {
            await _acceptLoopTask.ConfigureAwait(false);
        }
        catch
        {
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

    sealed class RewriteConnectionPair(
        TcpClient client,
        TcpClient server,
        IReadOnlyDictionary<string, CoordinatorMetadata> overrides,
        Action<string> recordSql,
        Action recordRewrite)
    {
        readonly TcpClient _client = client;
        readonly TcpClient _server = server;
        readonly IReadOnlyDictionary<string, CoordinatorMetadata> _overrides = overrides;
        readonly Action<string> _recordSql = recordSql;
        readonly Action _recordRewrite = recordRewrite;
        volatile bool _rewritePgxcNodeRows;

        internal async Task RunAsync(CancellationToken cancellationToken)
        {
            using (_client)
            using (_server)
            {
                var clientStream = _client.GetStream();
                var serverStream = _server.GetStream();

                var clientToServer = PumpClientToServerAsync(clientStream, serverStream, cancellationToken);
                var serverToClient = PumpServerToClientAsync(serverStream, clientStream, cancellationToken);

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

        async Task PumpClientToServerAsync(NetworkStream source, NetworkStream destination, CancellationToken cancellationToken)
        {
            var startup = await TryReadStartupPacketAsync(source, cancellationToken).ConfigureAwait(false);
            if (startup is null)
                return;

            await destination.WriteAsync(startup, cancellationToken).ConfigureAwait(false);
            await destination.FlushAsync(cancellationToken).ConfigureAwait(false);

            while (await TryReadTypedMessageAsync(source, cancellationToken).ConfigureAwait(false) is { } message)
            {
                if (TryGetFrontendSql(message, out var sql) &&
                    string.Equals(NormalizeSql(sql), NormalizeSql(SqlText.PgxcNodeRefresh), StringComparison.Ordinal))
                {
                    _recordSql(sql);
                    _rewritePgxcNodeRows = true;
                }

                await destination.WriteAsync(message, cancellationToken).ConfigureAwait(false);
                await destination.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        async Task PumpServerToClientAsync(NetworkStream source, NetworkStream destination, CancellationToken cancellationToken)
        {
            while (await TryReadTypedMessageAsync(source, cancellationToken).ConfigureAwait(false) is { } message)
            {
                if (_rewritePgxcNodeRows && message[0] == (byte)'D')
                {
                    message = RewritePgxcNodeDataRow(message, _overrides);
                    _recordRewrite();
                }

                await destination.WriteAsync(message, cancellationToken).ConfigureAwait(false);
                await destination.FlushAsync(cancellationToken).ConfigureAwait(false);

                if (_rewritePgxcNodeRows && message[0] == (byte)'Z')
                    _rewritePgxcNodeRows = false;
            }
        }

        static bool TryGetFrontendSql(byte[] message, out string sql)
        {
            sql = string.Empty;
            var payload = message.AsSpan(5);
            switch (message[0])
            {
            case (byte)'P':
            {
                var statementNameTerminator = payload.IndexOf((byte)0);
                if (statementNameTerminator < 0)
                    return false;

                var querySection = payload[(statementNameTerminator + 1)..];
                var sqlTerminator = querySection.IndexOf((byte)0);
                if (sqlTerminator < 0)
                    return false;

                sql = Encoding.UTF8.GetString(querySection[..sqlTerminator]);
                return true;
            }
            case (byte)'Q':
            {
                var sqlTerminator = payload.IndexOf((byte)0);
                if (sqlTerminator < 0)
                    return false;

                sql = Encoding.UTF8.GetString(payload[..sqlTerminator]);
                return true;
            }
            default:
                return false;
            }
        }

        static string NormalizeSql(string sql)
            => string.Join(" ", sql
                .Trim()
                .TrimEnd(';')
                .Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));

        static byte[] RewritePgxcNodeDataRow(byte[] message, IReadOnlyDictionary<string, CoordinatorMetadata> overrides)
        {
            var payload = message.AsSpan(5);
            var fieldCount = BinaryPrimitives.ReadInt16BigEndian(payload[..2]);
            if (fieldCount != 5)
                return message;

            var originalValues = new byte[fieldCount][];
            var offset = 2;
            for (var i = 0; i < fieldCount; i++)
            {
                var fieldLength = BinaryPrimitives.ReadInt32BigEndian(payload.Slice(offset, 4));
                offset += 4;
                if (fieldLength < 0)
                    return message;

                originalValues[i] = payload.Slice(offset, fieldLength).ToArray();
                offset += fieldLength;
            }

            var nodeName = Encoding.UTF8.GetString(originalValues[0]);
            if (!overrides.TryGetValue(nodeName, out var replacement))
                return message;

            var rewrittenValues = new byte[fieldCount][];
            rewrittenValues[0] = originalValues[0];
            rewrittenValues[1] = Encoding.ASCII.GetBytes(replacement.HostEndpoint.Host);
            rewrittenValues[2] = BuildPortBytes(replacement.HostEndpoint.Port, originalValues[2]);
            rewrittenValues[3] = Encoding.ASCII.GetBytes(replacement.EipEndpoint.Host);
            rewrittenValues[4] = BuildPortBytes(replacement.EipEndpoint.Port, originalValues[4]);
            return BuildDataRowMessage(rewrittenValues);
        }

        static byte[] BuildPortBytes(int port, byte[] originalValue)
        {
            if (originalValue.Length == 4)
            {
                var binary = new byte[4];
                BinaryPrimitives.WriteInt32BigEndian(binary, port);
                return binary;
            }

            return Encoding.ASCII.GetBytes(port.ToString());
        }

        static byte[] BuildDataRowMessage(byte[][] fieldValues)
        {
            var payloadLength = 2 + fieldValues.Sum(static value => 4 + value.Length);
            var message = new byte[1 + 4 + payloadLength];
            message[0] = (byte)'D';
            BinaryPrimitives.WriteInt32BigEndian(message.AsSpan(1, 4), 4 + payloadLength);
            BinaryPrimitives.WriteInt16BigEndian(message.AsSpan(5, 2), (short)fieldValues.Length);

            var offset = 7;
            foreach (var fieldValue in fieldValues)
            {
                BinaryPrimitives.WriteInt32BigEndian(message.AsSpan(offset, 4), fieldValue.Length);
                offset += 4;
                fieldValue.CopyTo(message.AsSpan(offset, fieldValue.Length));
                offset += fieldValue.Length;
            }

            return message;
        }

        static async Task<byte[]?> TryReadStartupPacketAsync(NetworkStream stream, CancellationToken cancellationToken)
        {
            var lengthBytes = new byte[4];
            if (!await TryReadExactAsync(stream, lengthBytes, cancellationToken).ConfigureAwait(false))
                return null;

            var length = BinaryPrimitives.ReadInt32BigEndian(lengthBytes);
            if (length < 4)
                throw new InvalidOperationException($"Invalid startup packet length {length}.");

            var packet = new byte[length];
            lengthBytes.CopyTo(packet.AsSpan(0, 4));
            if (length > 4 &&
                !await TryReadExactAsync(stream, packet.AsMemory(4, length - 4), cancellationToken).ConfigureAwait(false))
                return null;

            return packet;
        }

        static async Task<byte[]?> TryReadTypedMessageAsync(NetworkStream stream, CancellationToken cancellationToken)
        {
            var header = new byte[5];
            if (!await TryReadExactAsync(stream, header, cancellationToken).ConfigureAwait(false))
                return null;

            var length = BinaryPrimitives.ReadInt32BigEndian(header.AsSpan(1, 4));
            if (length < 4)
                throw new InvalidOperationException($"Invalid protocol message length {length}.");

            var message = new byte[1 + length];
            header.CopyTo(message, 0);
            if (length > 4 &&
                !await TryReadExactAsync(stream, message.AsMemory(5, length - 4), cancellationToken).ConfigureAwait(false))
                return null;

            return message;
        }

        static async Task<bool> TryReadExactAsync(NetworkStream stream, byte[] buffer, CancellationToken cancellationToken)
            => await TryReadExactAsync(stream, buffer.AsMemory(), cancellationToken).ConfigureAwait(false);

        static async Task<bool> TryReadExactAsync(NetworkStream stream, Memory<byte> buffer, CancellationToken cancellationToken)
        {
            var offset = 0;
            while (offset < buffer.Length)
            {
                var read = await stream.ReadAsync(buffer[offset..], cancellationToken).ConfigureAwait(false);
                if (read == 0)
                    return false;

                offset += read;
            }

            return true;
        }
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

    // 仅拒绝新连接，不影响已经建立好的连接。
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
        // 主动断掉当前所有转发中的连接，触发客户端侧的故障转移逻辑。
        foreach (var connection in _connections.Values)
            connection.Close();
    }

    async Task RunAcceptLoopAsync()
    {
        // 接受连接并把流量转发到真实目标；这是一个最小转发代理。
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
        // 每个客户端都建立一对 client/server socket，然后双向转发。
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
        // 用 RST 方式快速断开，避免测试因优雅关闭而错过故障时序。
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

        // 两个方向同时泵流，任一方向断开时就整体关闭这对 socket。
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
            // 关闭两端，确保客户端和服务端都感知到中断。
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
