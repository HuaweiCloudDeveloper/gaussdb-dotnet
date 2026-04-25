using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Data;
using System.Globalization;
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
case "open-auto-reconnect-transient-failure":
    await RunScenarioAsync("open-auto-reconnect-transient-failure", () => OpenAutoReconnectTransientFailureAsync(options));
    return;
case "priority-autobalance-preferred-cluster":
    await RunScenarioAsync("priority-autobalance-preferred-cluster", () => PriorityAutoBalancePreferredClusterAsync(options));
    return;
case "priority-loadbalance-false-sticks-first-seed":
    await RunScenarioAsync("priority-loadbalance-false-sticks-first-seed", () => PriorityLoadBalanceFalseSticksFirstSeedAsync(options));
    return;
case "priority-loadbalance-true-shuffles-within-cluster":
    await RunScenarioAsync("priority-loadbalance-true-shuffles-within-cluster", () => PriorityLoadBalanceTrueShufflesWithinClusterAsync(options));
    return;
case "priority-autobalance-true-ignores-loadbalancehosts":
    await RunScenarioAsync("priority-autobalance-true-ignores-loadbalancehosts", () => PriorityAutoBalanceTrueIgnoresLoadBalanceHostsAsync(options));
    return;
case "autobalance-balance-alias-matches-roundrobin":
    await RunScenarioAsync("autobalance-balance-alias-matches-roundrobin", () => AutoBalanceBalanceAliasMatchesRoundRobinAsync(options));
    return;
case "autobalance-priority-subset-routing":
    await RunScenarioAsync("autobalance-priority-subset-routing", () => AutoBalancePrioritySubsetRoutingAsync(options));
    return;
case "autobalance-shuffle-subset-routing":
    await RunScenarioAsync("autobalance-shuffle-subset-routing", () => AutoBalanceShuffleSubsetRoutingAsync(options));
    return;
case "autobalance-specified-seed-only":
    await RunScenarioAsync("autobalance-specified-seed-only", () => AutoBalanceSpecifiedSeedOnlyAsync(options));
    return;
case "autobalance-leastconn-preserves-order":
    await RunScenarioAsync("autobalance-leastconn-preserves-order", () => AutoBalanceLeastConnPreservesOrderAsync(options));
    return;
case "invalid-autobalance-rejected":
    await RunScenarioAsync("invalid-autobalance-rejected", () => InvalidAutoBalanceRejectedAsync(options));
    return;
case "invalid-autobalance-priority-bound-rejected":
    await RunScenarioAsync("invalid-autobalance-priority-bound-rejected", () => InvalidAutoBalancePriorityBoundRejectedAsync(options));
    return;
case "invalid-autobalance-shuffle-bound-rejected":
    await RunScenarioAsync("invalid-autobalance-shuffle-bound-rejected", () => InvalidAutoBalanceShuffleBoundRejectedAsync(options));
    return;
case "host-recheck-expiry-reprobes":
    await RunScenarioAsync("host-recheck-expiry-reprobes", () => HostRecheckExpiryReprobesAsync(options));
    return;
case "offline-cache-skips-immediate-reprobe":
    await RunScenarioAsync("offline-cache-skips-immediate-reprobe", () => OfflineCacheSkipsImmediateReprobeAsync(options));
    return;
case "host-recheck-zero-immediate-reprobe":
    await RunScenarioAsync("host-recheck-zero-immediate-reprobe", () => HostRecheckZeroImmediateReprobeAsync(options));
    return;
case "all-offline-fallback-recovered":
    await RunScenarioAsync("all-offline-fallback-recovered", () => AllOfflineFallbackRecoveredAsync(options));
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
case "cn-discovery-using-eip-selection":
    await RunScenarioAsync("cn-discovery-using-eip-selection", () => CnDiscoveryUsingEipSelectionAsync(options));
    return;
case "cn-discovery-refresh-disabled":
    await RunScenarioAsync("cn-discovery-refresh-disabled", () => CnDiscoveryRefreshDisabledAsync(options));
    return;
case "refresh-failure-throttled":
    await RunScenarioAsync("refresh-failure-throttled", () => RefreshFailureThrottledAsync(options));
    return;
case "cn-discovery-misconfigured-priority-seed-pollutes-cluster":
    await RunScenarioAsync("cn-discovery-misconfigured-priority-seed-pollutes-cluster", () => CnDiscoveryMisconfiguredPrioritySeedPollutesClusterAsync(options));
    return;
case "cn-discovery-unbound-fallback-seed-allows-foreign-node-adoption":
    await RunScenarioAsync("cn-discovery-unbound-fallback-seed-allows-foreign-node-adoption", () => CnDiscoveryUnboundFallbackSeedAllowsForeignNodeAdoptionAsync(options));
    return;
case "cn-discovery-bound-foreign-seed-does-not-join-preferred-cluster":
    await RunScenarioAsync("cn-discovery-bound-foreign-seed-does-not-join-preferred-cluster", () => CnDiscoveryBoundForeignSeedDoesNotJoinPreferredClusterAsync(options));
    return;
case "proxy-disconnect-no-replay":
    await RunScenarioAsync("proxy-disconnect-no-replay", () => ProxyDisconnectNoReplayAsync(options));
    return;
case "explicit-tx-admin-shutdown-no-replay":
    await RunScenarioAsync("explicit-tx-admin-shutdown-no-replay", () => ExplicitTransactionNoReplayAsync(options));
    return;
case "copy-export-disconnect-no-replay":
    await RunScenarioAsync("copy-export-disconnect-no-replay", () => CopyExportDisconnectNoReplayAsync(options));
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
case "seed-binding-rebind-using-eip-true":
    await RunScenarioAsync("seed-binding-rebind-using-eip-true", () => SeedBindingRebindScenarioAsync(options, usingEip: true));
    return;
case "seed-binding-rebind-using-eip-false":
    await RunScenarioAsync("seed-binding-rebind-using-eip-false", () => SeedBindingRebindScenarioAsync(options, usingEip: false));
    return;
case "seed-binding-rebind-state-check":
    await RunScenarioAsync("seed-binding-rebind-state-check", () => SeedBindingRebindStateCheckAsync(options));
    return;
case "single-cluster-enhanced-ha-without-priorityservers":
    await RunScenarioAsync("single-cluster-enhanced-ha-without-priorityservers", () => SingleClusterEnhancedHaWithoutPriorityServersAsync(options));
    return;
case "sql-error-no-reconnect":
    await RunScenarioAsync("sql-error-no-reconnect", () => SqlErrorNoReconnectAsync(options));
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
    Console.WriteLine("open-auto-reconnect-transient-failure");
    Console.WriteLine("priority-autobalance-preferred-cluster");
    Console.WriteLine("priority-loadbalance-false-sticks-first-seed");
    Console.WriteLine("priority-loadbalance-true-shuffles-within-cluster");
    Console.WriteLine("priority-autobalance-true-ignores-loadbalancehosts");
    Console.WriteLine("autobalance-balance-alias-matches-roundrobin");
    Console.WriteLine("autobalance-priority-subset-routing");
    Console.WriteLine("autobalance-shuffle-subset-routing");
    Console.WriteLine("autobalance-specified-seed-only");
    Console.WriteLine("autobalance-leastconn-preserves-order");
    Console.WriteLine("invalid-autobalance-rejected");
    Console.WriteLine("invalid-autobalance-priority-bound-rejected");
    Console.WriteLine("invalid-autobalance-shuffle-bound-rejected");
    Console.WriteLine("host-recheck-expiry-reprobes");
    Console.WriteLine("offline-cache-skips-immediate-reprobe");
    Console.WriteLine("host-recheck-zero-immediate-reprobe");
    Console.WriteLine("all-offline-fallback-recovered");
    Console.WriteLine("inspect-routing");
    Console.WriteLine("cn-discovery-roundrobin");
    Console.WriteLine("cn-discovery-proxy-seed-binding");
    Console.WriteLine("cn-discovery-forged-expanded-node-failover");
    Console.WriteLine("cn-discovery-forged-reachable-proxy-seed-binding");
    Console.WriteLine("cn-discovery-using-eip-selection");
    Console.WriteLine("cn-discovery-refresh-disabled");
    Console.WriteLine("refresh-failure-throttled");
    Console.WriteLine("cn-discovery-misconfigured-priority-seed-pollutes-cluster");
    Console.WriteLine("cn-discovery-unbound-fallback-seed-allows-foreign-node-adoption");
    Console.WriteLine("cn-discovery-bound-foreign-seed-does-not-join-preferred-cluster");
    Console.WriteLine("proxy-disconnect-no-replay");
    Console.WriteLine("explicit-tx-admin-shutdown-no-replay");
    Console.WriteLine("copy-export-disconnect-no-replay");
    Console.WriteLine("active-reader-disconnect-no-replay");
    Console.WriteLine("active-reader-second-command-in-progress");
    Console.WriteLine("timeout-no-replay");
    Console.WriteLine("seed-binding-rebind-using-eip-true");
    Console.WriteLine("seed-binding-rebind-using-eip-false");
    Console.WriteLine("seed-binding-rebind-state-check");
    Console.WriteLine("single-cluster-enhanced-ha-without-priorityservers");
    Console.WriteLine("sql-error-no-reconnect");
    Console.WriteLine("matrix");
}

static async Task RunMatrixAsync(Options options)
{
    // matrix 模式会把所有场景串起来跑一遍，并把每个场景的结果单独汇总。
    var scenarios = new (string Name, Func<Task> Run)[]
    {
        ("open-failover", () => OpenFailoverAsync(options)),
        ("open-auto-reconnect-transient-failure", () => OpenAutoReconnectTransientFailureAsync(options)),
        ("priority-autobalance-preferred-cluster", () => PriorityAutoBalancePreferredClusterAsync(options)),
        ("priority-loadbalance-false-sticks-first-seed", () => PriorityLoadBalanceFalseSticksFirstSeedAsync(options)),
        ("priority-loadbalance-true-shuffles-within-cluster", () => PriorityLoadBalanceTrueShufflesWithinClusterAsync(options)),
        ("priority-autobalance-true-ignores-loadbalancehosts", () => PriorityAutoBalanceTrueIgnoresLoadBalanceHostsAsync(options)),
        ("autobalance-balance-alias-matches-roundrobin", () => AutoBalanceBalanceAliasMatchesRoundRobinAsync(options)),
        ("autobalance-priority-subset-routing", () => AutoBalancePrioritySubsetRoutingAsync(options)),
        ("autobalance-shuffle-subset-routing", () => AutoBalanceShuffleSubsetRoutingAsync(options)),
        ("autobalance-specified-seed-only", () => AutoBalanceSpecifiedSeedOnlyAsync(options)),
        ("autobalance-leastconn-preserves-order", () => AutoBalanceLeastConnPreservesOrderAsync(options)),
        ("invalid-autobalance-rejected", () => InvalidAutoBalanceRejectedAsync(options)),
        ("invalid-autobalance-priority-bound-rejected", () => InvalidAutoBalancePriorityBoundRejectedAsync(options)),
        ("invalid-autobalance-shuffle-bound-rejected", () => InvalidAutoBalanceShuffleBoundRejectedAsync(options)),
        ("host-recheck-expiry-reprobes", () => HostRecheckExpiryReprobesAsync(options)),
        ("offline-cache-skips-immediate-reprobe", () => OfflineCacheSkipsImmediateReprobeAsync(options)),
        ("host-recheck-zero-immediate-reprobe", () => HostRecheckZeroImmediateReprobeAsync(options)),
        ("all-offline-fallback-recovered", () => AllOfflineFallbackRecoveredAsync(options)),
        ("cn-discovery-roundrobin", () => CnDiscoveryRoundRobinAsync(options)),
        ("cn-discovery-proxy-seed-binding", () => CnDiscoveryProxySeedBindingAsync(options)),
        ("cn-discovery-forged-expanded-node-failover", () => CnDiscoveryForgedExpandedNodeFailoverAsync(options)),
        ("cn-discovery-forged-reachable-proxy-seed-binding", () => CnDiscoveryForgedReachableProxySeedBindingAsync(options)),
        ("cn-discovery-using-eip-selection", () => CnDiscoveryUsingEipSelectionAsync(options)),
        ("cn-discovery-refresh-disabled", () => CnDiscoveryRefreshDisabledAsync(options)),
        ("refresh-failure-throttled", () => RefreshFailureThrottledAsync(options)),
        ("proxy-disconnect-no-replay", () => ProxyDisconnectNoReplayAsync(options)),
        ("explicit-tx-admin-shutdown-no-replay", () => ExplicitTransactionNoReplayAsync(options)),
        ("copy-export-disconnect-no-replay", () => CopyExportDisconnectNoReplayAsync(options)),
        ("active-reader-disconnect-no-replay", () => ActiveReaderNoReplayAsync(options)),
        ("active-reader-second-command-in-progress", () => ActiveReaderSecondCommandInProgressAsync(options)),
        ("timeout-no-replay", () => CommandTimeoutNoReplayAsync(options)),
        ("seed-binding-rebind-using-eip-true", () => SeedBindingRebindScenarioAsync(options, usingEip: true)),
        ("seed-binding-rebind-using-eip-false", () => SeedBindingRebindScenarioAsync(options, usingEip: false)),
        ("seed-binding-rebind-state-check", () => SeedBindingRebindStateCheckAsync(options)),
        ("single-cluster-enhanced-ha-without-priorityservers", () => SingleClusterEnhancedHaWithoutPriorityServersAsync(options)),
        ("sql-error-no-reconnect", () => SqlErrorNoReconnectAsync(options))
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
    // 先直连单个 seed，读取真实的 pgxc_node 元数据，建立“理论上应发现到哪些 CN”的基线。
    var seedTarget = options.Targets[0];
    var seedConnectionString = ConnectionStringUtil.BuildConnectionString(new[] { seedTarget }, options.BaseExtra, string.Empty);
    var seedBuilder = new GaussDBConnectionStringBuilder(seedConnectionString);

    Console.WriteLine($"seed-target={seedTarget}");
    Console.WriteLine($"using-eip={seedBuilder.UsingEip}");

    await using var seedConn = new GaussDBConnection(seedConnectionString);
    await seedConn.OpenAsync();

    // 当前 seed 所在 node_name + 当前活跃 coordinator 列表，后续都以此作为断言基准。
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
    // 对动态发现出的 endpoint 做一次直接可达性探测。
    // 如果这些地址从当前测试机根本不可达，驱动回退到 seed 才是正确行为。
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

    // 连续多次 Open，让驱动有机会在 round-robin 下逐步命中发现出的多个 CN。
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

    // 如果一个可达的发现节点都没有，则本场景的正确结果就是稳定留在 seed。
    if (directlyReachableDiscoveredNodes.Length == 0)
    {
        if (observedNodesOrdered.Length != 1 || observedNodesOrdered[0] != seedNodeName)
            throw new InvalidOperationException(
                $"No discovered non-seed endpoint was directly reachable, so the driver should have fallen back to the seed node only. observed=[{string.Join(",", observedNodesOrdered)}]");

        Console.WriteLine("validation-mode=seed-fallback");
        return;
    }

    // 只要存在可达的 discovered node，就应该能在多次 Open 后观察到它们被实际选中。
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
    // 这个场景验证：即使连接串里的 seed 先经过一层本地代理，驱动仍能把它绑定回正确的 node_name，
    // 并在动态 endpoint 可达时切到动态 endpoint；否则留在 seed fallback。
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

    // 用本地代理模拟“连接串里的 host != 数据库里暴露出来的真实地址”。
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

    // 先校验 node_name 绑定没丢，再根据 preferred endpoint 是否可达分别断言 direct-discovery 或 seed-fallback。
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
    // 这个场景把当前 seed 改写成不可达，同时把另一个真实 CN 改写成可达的 forged dynamic endpoint，
    // 验证驱动是否会按动态发现结果切到这个“扩容出来”的 CN。
    var seedRoutes = await LoadSeedRoutesAsync(options);
    if (seedRoutes.Length < 2)
        throw new InvalidOperationException("Forged expanded node scenario requires at least two seed targets.");

    var seedRoute = seedRoutes[0];
    var expandedRoute = seedRoutes[1];
    var coordinatorMetadata = await LoadCoordinatorMetadataByNodeNameAsync(options, seedRoute.Target);
    var seedCoordinator = coordinatorMetadata[seedRoute.NodeName];
    var expandedCoordinator = coordinatorMetadata[expandedRoute.NodeName];
    await using var expandedProxy = RealTcpFaultProxy.Start(expandedRoute.SeedEndpoint.Host, expandedRoute.SeedEndpoint.Port);
    var expandedEndpoint = ParseEndpoint(expandedProxy.Endpoint);
    var unreachableSeedEndpoint = GetUnreachableEndpoint();
    var overrides = new[]
    {
        new CoordinatorMetadata(
            seedRoute.NodeName,
            unreachableSeedEndpoint,
            unreachableSeedEndpoint,
            seedCoordinator.HostEndpoint,
            seedCoordinator.EipEndpoint),
        new CoordinatorMetadata(
            expandedRoute.NodeName,
            expandedEndpoint,
            expandedEndpoint,
            expandedCoordinator.HostEndpoint,
            expandedCoordinator.EipEndpoint)
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
        // 第一次 warm-open 负责触发刷新，把 forged endpoint 预热进候选集。
        var warmNodeName = await ExecuteScalarTextAsync(warmConn, "SELECT get_nodename();");
        var warmServerEndpoint = await ExecuteScalarTextAsync(warmConn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
        Console.WriteLine($"warm-open connected-via={warmConn.Host}:{warmConn.Port} server={warmServerEndpoint} node-name={warmNodeName}");
    }

    Console.WriteLine($"metadata-proxy-rewritten-rows={metadataProxy.RewrittenRowCount}");
    Console.WriteLine($"metadata-proxy-seen-sql={string.Join(" || ", metadataProxy.SeenSql)}");

    await metadataProxy.DisableAsync();
    Console.WriteLine($"disabled-metadata-proxy={metadataProxy.Endpoint}");

    // 关掉 metadata 入口后再次 Open，确认不是“偶然继续依赖旧 seed”，而是真的已经切到 forged discovered node。
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
    // 这个场景验证：当刷新结果把“当前 seed 对应的同一个 node_name”改写到另一个可达地址时，
    // 驱动应优先使用改写后的动态地址，而不是停留在 metadata proxy 上。
    var seedRoutes = await LoadSeedRoutesAsync(options);
    var seedRoute = seedRoutes[0];
    var coordinatorMetadata = await LoadCoordinatorMetadataByNodeNameAsync(options, seedRoute.Target);
    var seedCoordinator = coordinatorMetadata[seedRoute.NodeName];

    await using var forgedPreferredProxy = RealTcpFaultProxy.Start(seedRoute.SeedEndpoint.Host, seedRoute.SeedEndpoint.Port);
    var forgedPreferredEndpoint = ParseEndpoint(forgedPreferredProxy.Endpoint);
    var overrideCoordinator = new CoordinatorMetadata(
        seedRoute.NodeName,
        forgedPreferredEndpoint,
        forgedPreferredEndpoint,
        seedCoordinator.HostEndpoint,
        seedCoordinator.EipEndpoint);

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

static async Task CnDiscoveryUsingEipSelectionAsync(Options options)
{
    // 给同一个 node_name 同时伪造 host 列和 eip 列两套地址，
    // 分别验证 UsingEip=true/false 时到底会采用哪一列进入候选集。
    var seedRoutes = await LoadSeedRoutesAsync(options);
    var seedRoute = seedRoutes[0];
    var coordinatorMetadata = await LoadCoordinatorMetadataByNodeNameAsync(options, seedRoute.Target);
    var seedCoordinator = coordinatorMetadata[seedRoute.NodeName];

    await using var hostSideProxy = RealTcpFaultProxy.Start(seedRoute.SeedEndpoint.Host, seedRoute.SeedEndpoint.Port);
    await using var eipSideProxy = RealTcpFaultProxy.Start(seedRoute.SeedEndpoint.Host, seedRoute.SeedEndpoint.Port);
    var hostSideEndpoint = ParseEndpoint(hostSideProxy.Endpoint);
    var eipSideEndpoint = ParseEndpoint(eipSideProxy.Endpoint);

    Console.WriteLine($"seed-target={seedRoute.Target}");
    Console.WriteLine($"seed-node-name={seedRoute.NodeName}");
    Console.WriteLine($"forged-host-endpoint={hostSideEndpoint}");
    Console.WriteLine($"forged-eip-endpoint={eipSideEndpoint}");

    // 这里跑两次建连，除了 UsingEip 不同，其余条件完全一致。
    await OpenAndAssertPreferredEndpointAsync("using-eip-true", usingEip: true, eipSideEndpoint);
    await OpenAndAssertPreferredEndpointAsync("using-eip-false", usingEip: false, hostSideEndpoint);

    Console.WriteLine("validation-mode=using-eip-selection");

    async Task OpenAndAssertPreferredEndpointAsync(string label, bool usingEip, Endpoint expectedEndpoint)
    {
        // 同一行 metadata 同时覆盖 host/eip 两列，方便直接观察分支选择结果。
        await using var metadataProxy = PgMetadataRewriteProxy.Start(
            seedRoute.SeedEndpoint.Host,
            seedRoute.SeedEndpoint.Port,
            new[]
            {
                new CoordinatorMetadata(
                    seedRoute.NodeName,
                    hostSideEndpoint,
                    eipSideEndpoint,
                    seedCoordinator.HostEndpoint,
                    seedCoordinator.EipEndpoint)
            });

        var connectionString = ConnectionStringUtil.BuildConnectionString(
            new[] { metadataProxy.Endpoint },
            ApplyUsingEipToBaseExtra(options.BaseExtra, usingEip),
            "AutoBalance=roundrobin;RefreshCNIpListTime=30");
        Console.WriteLine($"{label}-metadata-proxy={metadataProxy.Endpoint} target={metadataProxy.Target}");
        Console.WriteLine($"{label}-connection-string={connectionString}");

        await using var dataSource = new GaussDBDataSourceBuilder(connectionString).BuildMultiHost();
        await using var conn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);

        // 既校验逻辑 node_name 不变，也校验物理 endpoint 命中的是期待那一列。
        var connectedNodeName = await ExecuteScalarTextAsync(conn, "SELECT get_nodename();");
        var serverEndpoint = await ExecuteScalarTextAsync(conn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
        var connectedEndpoint = new Endpoint(conn.Host!, conn.Port);
        Console.WriteLine($"{label} connected-via={connectedEndpoint} server={serverEndpoint} node-name={connectedNodeName}");
        Console.WriteLine($"{label}-metadata-proxy-rewritten-rows={metadataProxy.RewrittenRowCount}");
        Console.WriteLine($"{label}-metadata-proxy-seen-sql={string.Join(" || ", metadataProxy.SeenSql)}");

        if (connectedNodeName != seedRoute.NodeName)
            throw new InvalidOperationException(
                $"{label} expected node '{seedRoute.NodeName}', but connected to '{connectedNodeName}'.");

        if (connectedEndpoint.ToString() != expectedEndpoint.ToString())
            throw new InvalidOperationException(
                $"{label} expected endpoint {expectedEndpoint}, but connected via {connectedEndpoint}.");

        if (connectedEndpoint.ToString() == metadataProxy.Endpoint)
            throw new InvalidOperationException($"{label} unexpectedly stayed on the metadata rewrite proxy.");

        if (metadataProxy.RewrittenRowCount == 0)
            throw new InvalidOperationException($"{label} expected CN discovery to read and rewrite pgxc_node rows.");
    }
}

static async Task CnDiscoveryRefreshDisabledAsync(Options options)
{
    // 即便 metadata 里伪造了一个可达的 dynamic endpoint，只要 RefreshCNIpListTime=0，
    // 驱动就不应触发刷新，也不应采用这个 forged endpoint。
    var seedRoutes = await LoadSeedRoutesAsync(options);
    var seedRoute = seedRoutes[0];

    await using var forgedPreferredProxy = RealTcpFaultProxy.Start(seedRoute.SeedEndpoint.Host, seedRoute.SeedEndpoint.Port);
    var forgedPreferredEndpoint = ParseEndpoint(forgedPreferredProxy.Endpoint);

    await using var metadataProxy = PgMetadataRewriteProxy.Start(
        seedRoute.SeedEndpoint.Host,
        seedRoute.SeedEndpoint.Port,
        new[] { new CoordinatorMetadata(seedRoute.NodeName, forgedPreferredEndpoint, forgedPreferredEndpoint) });

    var connectionString = ConnectionStringUtil.BuildConnectionString(
        new[] { metadataProxy.Endpoint },
        options.BaseExtra,
        "AutoBalance=roundrobin;RefreshCNIpListTime=0");

    Console.WriteLine($"seed-target={seedRoute.Target}");
    Console.WriteLine($"seed-node-name={seedRoute.NodeName}");
    Console.WriteLine($"metadata-proxy={metadataProxy.Endpoint} target={metadataProxy.Target}");
    Console.WriteLine($"forged-preferred-endpoint={forgedPreferredEndpoint}");
    Console.WriteLine($"ConnectionString={connectionString}");

    await using var dataSource = new GaussDBDataSourceBuilder(connectionString).BuildMultiHost();
    await using var conn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);

    var connectedNodeName = await ExecuteScalarTextAsync(conn, "SELECT get_nodename();");
    var serverEndpoint = await ExecuteScalarTextAsync(conn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
    var connectedEndpoint = new Endpoint(conn.Host!, conn.Port);
    Console.WriteLine($"connected-via={connectedEndpoint} server={serverEndpoint} node-name={connectedNodeName}");
    Console.WriteLine($"metadata-proxy-rewritten-rows={metadataProxy.RewrittenRowCount}");
    Console.WriteLine($"metadata-proxy-seen-sql={string.Join(" || ", metadataProxy.SeenSql)}");

    if (connectedNodeName != seedRoute.NodeName)
        throw new InvalidOperationException(
            $"Expected disabled refresh scenario to stay on seed node '{seedRoute.NodeName}', but connected to '{connectedNodeName}'.");

    if (connectedEndpoint.ToString() != metadataProxy.Endpoint)
        throw new InvalidOperationException(
            $"RefreshCNIpListTime=0 should not use forged discovered endpoint {forgedPreferredEndpoint}; connected via {connectedEndpoint}.");

    if (metadataProxy.RewrittenRowCount != 0)
        throw new InvalidOperationException("RefreshCNIpListTime=0 unexpectedly triggered pgxc_node row rewriting.");

    if (metadataProxy.SeenSql.Length != 0)
        throw new InvalidOperationException(
            $"RefreshCNIpListTime=0 unexpectedly queried pgxc_node: {string.Join(" || ", metadataProxy.SeenSql)}");

    Console.WriteLine("validation-mode=refresh-disabled-seed-only");
}

static async Task RefreshFailureThrottledAsync(Options options)
{
    // metadata proxy 会拦截并断开 pgxc_node 刷新 SQL，但 seed 正常连接本身仍可成功。
    // 这个场景验证刷新失败后是否会在 TTL 窗口内被节流，而不是每次 Open 都重新刷一次。
    var seedRoute = (await LoadSeedRoutesAsync(options))[0];
    await using var metadataProxy = PgMetadataRewriteProxy.StartRejectingRefresh(
        seedRoute.SeedEndpoint.Host,
        seedRoute.SeedEndpoint.Port);

    var refreshSeconds = Math.Max(10, options.RefreshSecondsForScenario);
    var connectionString = ConnectionStringUtil.BuildConnectionString(
        new[] { metadataProxy.Endpoint },
        options.BaseExtra,
        $"AutoBalance=roundrobin;RefreshCNIpListTime={refreshSeconds}");

    Console.WriteLine($"seed-target={seedRoute.Target}");
    Console.WriteLine($"metadata-proxy={metadataProxy.Endpoint} target={metadataProxy.Target}");
    Console.WriteLine($"ConnectionString={connectionString}");

    await using var dataSource = new GaussDBDataSourceBuilder(connectionString).BuildMultiHost();
    // 第一次 Open 触发一次刷新尝试，刷新失败后仍应通过 seed 建连成功。
    await using (var firstConn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any))
    {
        var server = await ExecuteScalarTextAsync(firstConn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
        Console.WriteLine($"first-open connected-via={firstConn.Host}:{firstConn.Port} server={server}");
    }

    var refreshCountAfterFirst = metadataProxy.SeenSql.Length;

    // 第二次 Open 仍在刷新窗口内，不应再次触发 refresh SQL。
    await using (var secondConn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any))
    {
        var server = await ExecuteScalarTextAsync(secondConn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
        Console.WriteLine($"second-open connected-via={secondConn.Host}:{secondConn.Port} server={server}");
    }

    var refreshCountAfterSecond = metadataProxy.SeenSql.Length;
    Console.WriteLine($"refresh-sql-after-first={refreshCountAfterFirst}");
    Console.WriteLine($"refresh-sql-after-second={refreshCountAfterSecond}");
    Console.WriteLine($"metadata-proxy-seen-sql={string.Join(" || ", metadataProxy.SeenSql)}");

    if (refreshCountAfterFirst != 1 || refreshCountAfterSecond != 1)
    {
        throw new InvalidOperationException(
            $"Refresh failures should be throttled within the RefreshCNIpListTime window. observed-first={refreshCountAfterFirst} observed-second={refreshCountAfterSecond}");
    }

    Console.WriteLine("validation-mode=refresh-failure-throttled");
}

static async Task CnDiscoveryMisconfiguredPrioritySeedPollutesClusterAsync(Options options)
{
    // 这是一个调试型场景：
    // 故意把优先簇 seed 指到别的簇，先制造一次错误吸纳，
    // 再验证真实 seed 后绑定 node_name 后，驱动是否能把污染掉的动态 endpoint 纠正出去。
    // 这个场景验证 fix rebind bug：
    // 即使优先 seed 一开始被错误配置到别的簇，后续真实 seed 一旦明确识别出 node_name -> cluster，
    // 也要把先前误吸纳到首选簇的外簇节点纠正回自己的簇，不再让伪造动态地址留在首选簇里。
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

    // 这里让“连接串里排第一的 seed”实际上连到 foreignClusterSeed，并把每个 node_name 都映射到本地代理。
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
        // 连续两次 warm-open，目的是先把错误快照和错误归属喂进驱动内部状态。
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

        // 再次建连时，如果还命中 forged endpoint，就说明簇污染没有被修正。
        await using var afterDisableConn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);
        var observedNodeName = await ExecuteScalarTextAsync(afterDisableConn, "SELECT get_nodename();");
        var observedServerEndpoint = await ExecuteScalarTextAsync(afterDisableConn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
        var observedEndpoint = new Endpoint(afterDisableConn.Host!, afterDisableConn.Port);
        Console.WriteLine($"after-disable connected-via={observedEndpoint} server={observedServerEndpoint} node-name={observedNodeName}");

        var forgedEndpointKeys = forgedRoutes.Select(static route => route.ProxyEndpoint.ToString()).ToHashSet(StringComparer.Ordinal);
        var pollutionObserved = forgedEndpointKeys.Contains(observedEndpoint.ToString());
        Console.WriteLine($"pollution-observed={pollutionObserved}");

        if (pollutionObserved)
            throw new InvalidOperationException(
                $"Expected the rebind fix to prevent polluted forged endpoints from remaining in the preferred cluster, but connected via forged endpoint {observedEndpoint}.");

        Console.WriteLine("rebind-fix-observed=True");
    }
}

static async Task CnDiscoveryUnboundFallbackSeedAllowsForeignNodeAdoptionAsync(Options options)
{
    // 这个调试场景故意让 fallback seed 一直无法绑定 node_name，
    // 观察 preferred cluster 是否会错误吸纳 foreign node。
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
    // 两次 warm-open 的目的都是把错误发现结果预热进当前数据源。
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

    // 这次建连如果直接打到 foreignProxyEndpoint，就说明 foreign node 被错误吸入了 preferred cluster。
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

static async Task CnDiscoveryBoundForeignSeedDoesNotJoinPreferredClusterAsync(Options options)
{
    // 和上一个场景相反，这里保留了一个可被识别的 fallback seed，
    // 用来验证：只要 fallback seed 后续能正确绑定 node_name，它就不会再被错误吸入 preferred cluster。
    // 这个场景保留了可被后续明确识别的 fallback seed，用来验证：
    // 一旦 seed 能够确认 node_name -> cluster 归属，驱动会纠正先前动态发现造成的误归簇。
    var seedRoutes = await LoadSeedRoutesAsync(options);
    if (seedRoutes.Length < 2)
        throw new InvalidOperationException("Bound foreign seed scenario requires at least two real seed targets.");

    var preferredSeed = seedRoutes[0];
    var fallbackSeed = seedRoutes[1];
    var deadPrimaryEndpoint = GetUnreachableEndpoint();

    await using var fallbackRejectProbe = RejectingEndpointProbe.Start();
    var fallbackProbeEndpoint = ParseEndpoint(fallbackRejectProbe.Endpoint);

    await using var metadataProxy = PgMetadataRewriteProxy.Start(
        preferredSeed.SeedEndpoint.Host,
        preferredSeed.SeedEndpoint.Port,
        new[]
        {
            new CoordinatorMetadata(preferredSeed.NodeName, deadPrimaryEndpoint, deadPrimaryEndpoint),
            new CoordinatorMetadata(fallbackSeed.NodeName, fallbackProbeEndpoint, fallbackProbeEndpoint)
        });

    var discoveryConnectionString = ConnectionStringUtil.BuildConnectionString(
        new[] { metadataProxy.Endpoint, fallbackRejectProbe.Endpoint },
        options.BaseExtra,
        "PriorityServers=1;AutoBalance=roundrobin;RefreshCNIpListTime=30");

    Console.WriteLine($"preferred-seed={preferredSeed.Target} node={preferredSeed.NodeName}");
    Console.WriteLine($"fallback-seed={fallbackSeed.Target} node={fallbackSeed.NodeName}");
    Console.WriteLine($"metadata-proxy={metadataProxy.Endpoint} target={metadataProxy.Target}");
    Console.WriteLine($"rewritten-primary-endpoint={deadPrimaryEndpoint}");
    Console.WriteLine($"rewritten-fallback-seed={fallbackRejectProbe.Endpoint}");
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
        throw new InvalidOperationException("Expected the second open to refresh pgxc_node on the preferred cluster, but no rows were rewritten.");

    await metadataProxy.DisableAsync();
    Console.WriteLine($"disabled-preferred-metadata-proxy={metadataProxy.Endpoint}");

    Exception? captured = null;
    try
    {
        await using var afterDisableConn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);
        var observedNodeName = await ExecuteScalarTextAsync(afterDisableConn, "SELECT get_nodename();");
        var observedServerEndpoint = await ExecuteScalarTextAsync(afterDisableConn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
        throw new InvalidOperationException(
            $"Connection unexpectedly succeeded after disabling the preferred seed. connected-via={afterDisableConn.Host}:{afterDisableConn.Port} server={observedServerEndpoint} node-name={observedNodeName}");
    }
    catch (Exception ex)
    {
        captured = ex;
    }

    if (captured is InvalidOperationException invalidOperationException &&
        invalidOperationException.Message.StartsWith("Connection unexpectedly succeeded", StringComparison.Ordinal))
        throw captured;

    Console.WriteLine($"captured={captured!.GetType().Name}: {captured.Message}");
    Console.WriteLine($"fallback-reject-connection-count={fallbackRejectProbe.ConnectionCount}");

    if (fallbackRejectProbe.ConnectionCount != 1)
        throw new InvalidOperationException(
            $"Expected the bound fallback seed to stay only in its own cluster and be attempted exactly once. observed-attempts={fallbackRejectProbe.ConnectionCount}");

    Console.WriteLine("rebind-fix-observed=True");
}

static async Task OpenFailoverAsync(Options options)
{
    // 验证 Open 阶段的基础 failover：
    // 第一个 seed 被主动掐断后，驱动应继续尝试后续节点，而不是卡死在首个 seed。
    // 验证 Open 阶段能在主节点不可达时切到备节点，而不是卡死在第一个 seed 上。
    await using var proxyGroup = new ProxyGroup(options.Targets);
    var disabled = proxyGroup.GetByIndex(0);
    await disabled.DisableAsync();

    // 这里显式打开 AutoReconnect，是为了让 Open 阶段路径和正式 HA 行为一致。
    var connectionString = proxyGroup.ConnectionString(options.BaseExtra, "PriorityServers=2;AutoReconnect=true;MaxReconnects=3");
    Console.WriteLine($"ConnectionString={connectionString}");

    await using var conn = new GaussDBConnection(connectionString);
    await conn.OpenAsync();

    var server = await ExecuteScalarTextAsync(conn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
    Console.WriteLine($"connected-via={conn.Host}:{conn.Port} server={server}");

    if (conn.Port == disabled.Port)
        throw new InvalidOperationException("Open fallback did not skip the disabled primary seed.");
}

static async Task OpenAutoReconnectTransientFailureAsync(Options options)
{
    // 验证 Open 阶段“瞬时失败后重试一次即可恢复”的能力。
    // 代理只拒绝第一条 TCP 连接，后续连接恢复正常。
    var seedRoute = (await LoadSeedRoutesAsync(options))[0];
    await using var transientProxy = RealTcpFaultProxy.Start(
        seedRoute.SeedEndpoint.Host,
        seedRoute.SeedEndpoint.Port,
        initialRejectedConnectionCount: 1);

    var connectionString = ConnectionStringUtil.BuildConnectionString(
        new[] { transientProxy.Endpoint },
        options.BaseExtra,
        "AutoReconnect=true;MaxReconnects=3");
    Console.WriteLine($"seed-target={seedRoute.Target}");
    Console.WriteLine($"transient-proxy={transientProxy.Endpoint} target={transientProxy.Target}");
    Console.WriteLine($"ConnectionString={connectionString}");

    // 如果 AutoReconnect 生效，Open 应成功且代理连接次数至少为 2。
    await using var conn = new GaussDBConnection(connectionString);
    await conn.OpenAsync();

    var nodeName = await ExecuteScalarTextAsync(conn, "SELECT get_nodename();");
    var server = await ExecuteScalarTextAsync(conn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
    Console.WriteLine($"connected-via={conn.Host}:{conn.Port} server={server} node-name={nodeName}");
    Console.WriteLine($"proxy-connection-count={transientProxy.ConnectionCount}");

    if (transientProxy.ConnectionCount < 2)
        throw new InvalidOperationException(
            $"Expected Open auto-reconnect to retry after the transient failure. observed-connections={transientProxy.ConnectionCount}");

    if (conn.Port != transientProxy.Port)
        throw new InvalidOperationException($"Expected to reconnect through {transientProxy.Endpoint}, but connected via {conn.Host}:{conn.Port}.");
}

static async Task PriorityAutoBalancePreferredClusterAsync(Options options)
{
    // 验证 PriorityServers + AutoBalance=priority1 的组合行为：
    // 正常时优先命中主 AZ 第一个 seed；该 seed 故障后仍先留在主 AZ 内切换。
    if (options.Targets.Length < 3)
        throw new InvalidOperationException("Priority auto-balance scenario requires at least three seed targets.");

    await using var proxyGroup = new ProxyGroup(options.Targets);
    var preferredPrimary = proxyGroup.GetByIndex(0);
    var preferredPeer = proxyGroup.GetByIndex(1);
    var fallbackCluster = proxyGroup.GetByIndex(2);
    var connectionString = proxyGroup.ConnectionString(
        options.BaseExtra,
        "PriorityServers=2;AutoBalance=priority1;RefreshCNIpListTime=0");
    Console.WriteLine($"ConnectionString={connectionString}");
    Console.WriteLine($"preferred-primary={preferredPrimary.Endpoint}");
    Console.WriteLine($"preferred-peer={preferredPeer.Endpoint}");
    Console.WriteLine($"fallback-cluster={fallbackCluster.Endpoint}");

    await using var dataSource = new GaussDBDataSourceBuilder(connectionString).BuildMultiHost();
    // 第一次先看正常命中路径。
    await using (var preferredConn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any))
    {
        var server = await ExecuteScalarTextAsync(preferredConn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
        Console.WriteLine($"preferred-open connected-via={preferredConn.Host}:{preferredConn.Port} server={server}");

        if (preferredConn.Port != preferredPrimary.Port)
            throw new InvalidOperationException(
                $"AutoBalance=priority1 should prefer the first seed in the preferred cluster. connected-via={preferredConn.Host}:{preferredConn.Port}");
    }

    // 再掐掉主 AZ 的第一个 seed，验证 failover 是否仍停留在当前优先簇。
    await preferredPrimary.DisableAsync();
    Console.WriteLine($"disabled-preferred-primary={preferredPrimary.Endpoint}");

    await using var failoverConn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);
    var failoverServer = await ExecuteScalarTextAsync(failoverConn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
    Console.WriteLine($"failover-open connected-via={failoverConn.Host}:{failoverConn.Port} server={failoverServer}");

    if (failoverConn.Port != preferredPeer.Port)
        throw new InvalidOperationException(
            $"Expected failover to stay inside the preferred cluster and use {preferredPeer.Endpoint}, but connected via {failoverConn.Host}:{failoverConn.Port}.");

    if (failoverConn.Port == fallbackCluster.Port)
        throw new InvalidOperationException("Failover unexpectedly jumped to the fallback cluster before exhausting the preferred cluster.");
}

static async Task PriorityLoadBalanceFalseSticksFirstSeedAsync(Options options)
{
    // 验证未显式设置 AutoBalance 时，LoadBalanceHosts=false 会保持当前簇内的候选顺序。
    var seedRoutes = await LoadSeedRoutesAsync(options);
    var preferredRoutes = GetPreferredRoutesForPriorityScenario(seedRoutes, options);
    var fallbackRoutes = GetFallbackRoutesForPriorityScenario(seedRoutes, options);
    var connectionString = ConnectionStringUtil.BuildConnectionString(
        options.Targets,
        options.BaseExtra,
        $"PriorityServers={options.PriorityServersForScenario};LoadBalanceHosts=false");

    Console.WriteLine($"ConnectionString={connectionString}");
    Console.WriteLine($"preferred-seeds={string.Join(",", preferredRoutes.Select(static route => route.Target))}");
    Console.WriteLine($"fallback-seeds={string.Join(",", fallbackRoutes.Select(static route => route.Target))}");

    // 连续多次 Open，观察是否始终钉在第一个 preferred seed。
    var observations = await SampleOpenObservationsAsync(connectionString, Math.Max(6, preferredRoutes.Length * 2));
    DumpObservations(observations);

    var expectedEndpoint = preferredRoutes[0].SeedEndpoint.ToString();
    if (observations.Any(observation => observation.ConnectedEndpoint != expectedEndpoint))
    {
        throw new InvalidOperationException(
            $"LoadBalanceHosts=false and AutoBalance=false should stay on the first preferred seed. expected={expectedEndpoint} observed=[{string.Join(",", observations.Select(static observation => observation.ConnectedEndpoint))}]");
    }

    Console.WriteLine("validation-mode=priority-loadbalance-false-first-seed");
}

static async Task PriorityLoadBalanceTrueShufflesWithinClusterAsync(Options options)
{
    // 验证 JDBC 风格 legacy shuffle：
    // LoadBalanceHosts=true 只在当前优先簇内打散，不应越级跑到 fallback cluster。
    var seedRoutes = await LoadSeedRoutesAsync(options);
    var preferredRoutes = GetPreferredRoutesForPriorityScenario(seedRoutes, options);
    var fallbackRoutes = GetFallbackRoutesForPriorityScenario(seedRoutes, options);
    var connectionString = ConnectionStringUtil.BuildConnectionString(
        options.Targets,
        options.BaseExtra,
        $"PriorityServers={options.PriorityServersForScenario};LoadBalanceHosts=true");

    Console.WriteLine($"ConnectionString={connectionString}");
    Console.WriteLine($"preferred-seeds={string.Join(",", preferredRoutes.Select(static route => route.Target))}");
    Console.WriteLine($"fallback-seeds={string.Join(",", fallbackRoutes.Select(static route => route.Target))}");

    var observations = await SampleOpenObservationsAsync(connectionString, Math.Max(16, preferredRoutes.Length * 6));
    DumpObservations(observations);

    var preferredEndpoints = preferredRoutes.Select(static route => route.SeedEndpoint.ToString()).ToHashSet(StringComparer.Ordinal);
    var fallbackEndpoints = fallbackRoutes.Select(static route => route.SeedEndpoint.ToString()).ToHashSet(StringComparer.Ordinal);
    var observedEndpoints = observations.Select(static observation => observation.ConnectedEndpoint).Distinct(StringComparer.Ordinal).OrderBy(static x => x, StringComparer.Ordinal).ToArray();

    if (observations.Any(observation => !preferredEndpoints.Contains(observation.ConnectedEndpoint)))
    {
        throw new InvalidOperationException(
            $"LoadBalanceHosts=true should only shuffle inside the preferred cluster. preferred=[{string.Join(",", preferredEndpoints.OrderBy(static x => x, StringComparer.Ordinal))}] observed=[{string.Join(",", observedEndpoints)}]");
    }

    if (observations.Any(observation => fallbackEndpoints.Contains(observation.ConnectedEndpoint)))
    {
        throw new InvalidOperationException(
            $"LoadBalanceHosts=true unexpectedly used a fallback seed. fallback=[{string.Join(",", fallbackEndpoints.OrderBy(static x => x, StringComparer.Ordinal))}] observed=[{string.Join(",", observedEndpoints)}]");
    }

    if (preferredRoutes.Length > 1 && observedEndpoints.Length < 2)
    {
        throw new InvalidOperationException(
            $"LoadBalanceHosts=true should distribute opens across multiple preferred seeds over repeated attempts. observed=[{string.Join(",", observedEndpoints)}]");
    }

    Console.WriteLine("validation-mode=priority-loadbalance-true-preferred-cluster-shuffle");
}

static async Task PriorityAutoBalanceTrueIgnoresLoadBalanceHostsAsync(Options options)
{
    // 验证显式 AutoBalance=true 时，LoadBalanceHosts 不再参与决策。
    // 两组观测序列应该完全一致。
    var preferredRoutes = GetPreferredRoutesForPriorityScenario(await LoadSeedRoutesAsync(options), options);
    var preferredEndpoints = preferredRoutes.Select(static route => route.SeedEndpoint.ToString()).ToHashSet(StringComparer.Ordinal);

    var observedWithLoadBalanceFalse = await ObservePriorityAutoBalanceTrueAsync(options, loadBalanceHosts: false);
    var observedWithLoadBalanceTrue = await ObservePriorityAutoBalanceTrueAsync(options, loadBalanceHosts: true);

    var falseSequence = observedWithLoadBalanceFalse.Select(static observation => observation.ConnectedEndpoint).ToArray();
    var trueSequence = observedWithLoadBalanceTrue.Select(static observation => observation.ConnectedEndpoint).ToArray();

    if (!falseSequence.SequenceEqual(trueSequence, StringComparer.Ordinal))
    {
        throw new InvalidOperationException(
            $"AutoBalance=true should ignore LoadBalanceHosts and produce the same observed connection sequence. loadBalanceHosts=false=[{string.Join(",", falseSequence)}] loadBalanceHosts=true=[{string.Join(",", trueSequence)}]");
    }

    if (observedWithLoadBalanceFalse.Any(observation => !preferredEndpoints.Contains(observation.ConnectedEndpoint)) ||
        observedWithLoadBalanceTrue.Any(observation => !preferredEndpoints.Contains(observation.ConnectedEndpoint)))
    {
        throw new InvalidOperationException(
            $"AutoBalance=true should stay inside the preferred cluster regardless of LoadBalanceHosts. preferred=[{string.Join(",", preferredEndpoints.OrderBy(static x => x, StringComparer.Ordinal))}] false=[{string.Join(",", falseSequence)}] true=[{string.Join(",", trueSequence)}]");
    }

    if (preferredEndpoints.Count > 1 &&
        (falseSequence.Distinct(StringComparer.Ordinal).Count() < 2 || trueSequence.Distinct(StringComparer.Ordinal).Count() < 2))
    {
        throw new InvalidOperationException(
            $"AutoBalance=true should rotate across multiple preferred seeds over repeated opens. false=[{string.Join(",", falseSequence)}] true=[{string.Join(",", trueSequence)}]");
    }

    Console.WriteLine("validation-mode=priority-autobalance-true-ignores-loadbalancehosts");
}

static async Task AutoBalanceBalanceAliasMatchesRoundRobinAsync(Options options)
{
    // 验证 roundrobin / true / balance 三个别名在真实库上的行为完全一致。
    var preferredRoutes = GetPreferredRoutesForPriorityScenario(await LoadSeedRoutesAsync(options), options);
    var preferredEndpoints = preferredRoutes.Select(static route => route.SeedEndpoint.ToString()).ToHashSet(StringComparer.Ordinal);

    var roundRobin = await ObserveAutoBalanceModeAsync(options, "roundrobin", attempts: preferredRoutes.Length * 2);
    var autoTrue = await ObserveAutoBalanceModeAsync(options, "true", attempts: preferredRoutes.Length * 2);
    var balance = await ObserveAutoBalanceModeAsync(options, "balance", attempts: preferredRoutes.Length * 2);

    var roundRobinSequence = roundRobin.Select(static observation => observation.ConnectedEndpoint).ToArray();
    var autoTrueSequence = autoTrue.Select(static observation => observation.ConnectedEndpoint).ToArray();
    var balanceSequence = balance.Select(static observation => observation.ConnectedEndpoint).ToArray();

    if (!roundRobinSequence.SequenceEqual(autoTrueSequence, StringComparer.Ordinal) ||
        !roundRobinSequence.SequenceEqual(balanceSequence, StringComparer.Ordinal))
    {
        throw new InvalidOperationException(
            $"AutoBalance=roundrobin/true/balance should behave identically. roundrobin=[{string.Join(",", roundRobinSequence)}] true=[{string.Join(",", autoTrueSequence)}] balance=[{string.Join(",", balanceSequence)}]");
    }

    if (roundRobin.Any(observation => !preferredEndpoints.Contains(observation.ConnectedEndpoint)))
    {
        throw new InvalidOperationException(
            $"Round-robin aliases should stay inside the preferred cluster. preferred=[{string.Join(",", preferredEndpoints.OrderBy(static x => x, StringComparer.Ordinal))}] observed=[{string.Join(",", roundRobinSequence)}]");
    }

    if (preferredEndpoints.Count > 1 && roundRobinSequence.Distinct(StringComparer.Ordinal).Count() < 2)
    {
        throw new InvalidOperationException(
            $"Round-robin aliases should rotate across multiple preferred seeds. observed=[{string.Join(",", roundRobinSequence)}]");
    }

    Console.WriteLine("validation-mode=autobalance-balance-alias-roundrobin");
}

static async Task AutoBalancePrioritySubsetRoutingAsync(Options options)
{
    // 验证 priorityN：
    // 正常时只打优先子集；优先子集里的首节点故障后，仍先留在同簇内切到其他节点。
    if (options.Targets.Length < 3)
        throw new InvalidOperationException("Priority subset routing scenario requires at least three seed targets.");

    await using var proxyGroup = new ProxyGroup(options.Targets);
    var firstPreferred = proxyGroup.GetByIndex(0);
    var secondPreferred = proxyGroup.GetByIndex(1);
    var fallbackSeed = proxyGroup.GetByIndex(2);
    var connectionString = proxyGroup.ConnectionString(
        options.BaseExtra,
        "PriorityServers=2;AutoBalance=priority1;RefreshCNIpListTime=0");

    Console.WriteLine($"ConnectionString={connectionString}");
    Console.WriteLine($"first-preferred={firstPreferred.Endpoint}");
    Console.WriteLine($"second-preferred={secondPreferred.Endpoint}");
    Console.WriteLine($"fallback-seed={fallbackSeed.Endpoint}");

    var preferredObservations = await SampleOpenObservationsAsync(connectionString, 4);
    DumpObservations(preferredObservations);
    if (preferredObservations.Any(observation => observation.ConnectedEndpoint != firstPreferred.Endpoint))
    {
        throw new InvalidOperationException(
            $"AutoBalance=priority1 should keep using the priority subset while it stays healthy. expected={firstPreferred.Endpoint} observed=[{string.Join(",", preferredObservations.Select(static observation => observation.ConnectedEndpoint))}]");
    }

    await firstPreferred.DisableAsync();
    Console.WriteLine($"disabled-first-preferred={firstPreferred.Endpoint}");

    var failoverObservations = await SampleOpenObservationsAsync(connectionString, 3);
    DumpObservations(failoverObservations);
    if (failoverObservations.Any(observation => observation.ConnectedEndpoint != secondPreferred.Endpoint))
    {
        throw new InvalidOperationException(
            $"After the priority subset fails, AutoBalance=priority1 should stay inside the preferred cluster and use {secondPreferred.Endpoint}. observed=[{string.Join(",", failoverObservations.Select(static observation => observation.ConnectedEndpoint))}]");
    }

    if (failoverObservations.Any(observation => observation.ConnectedEndpoint == fallbackSeed.Endpoint))
        throw new InvalidOperationException("Priority subset routing unexpectedly jumped to the fallback cluster before exhausting the preferred cluster.");

    Console.WriteLine("validation-mode=autobalance-priority-subset-routing");
}

static async Task AutoBalanceShuffleSubsetRoutingAsync(Options options)
{
    // 验证 shuffleN：
    // 只在优先子集内 shuffle，健康状态下不应触达非优先子集。
    var seedRoutes = await LoadSeedRoutesAsync(options);
    if (seedRoutes.Length < 3)
        throw new InvalidOperationException("Shuffle subset routing scenario requires at least three seed targets.");

    var prioritySubset = seedRoutes.Take(2).Select(static route => route.SeedEndpoint.ToString()).ToHashSet(StringComparer.Ordinal);
    var nonPrioritySubset = seedRoutes.Skip(2).Select(static route => route.SeedEndpoint.ToString()).ToHashSet(StringComparer.Ordinal);
    var connectionString = ConnectionStringUtil.BuildConnectionString(
        options.Targets,
        options.BaseExtra,
        "AutoBalance=shuffle2;RefreshCNIpListTime=0");

    Console.WriteLine($"ConnectionString={connectionString}");
    Console.WriteLine($"priority-subset={string.Join(",", prioritySubset.OrderBy(static x => x, StringComparer.Ordinal))}");
    Console.WriteLine($"non-priority-subset={string.Join(",", nonPrioritySubset.OrderBy(static x => x, StringComparer.Ordinal))}");

    var observations = await SampleOpenObservationsAsync(connectionString, 16);
    DumpObservations(observations);

    if (observations.Any(observation => !prioritySubset.Contains(observation.ConnectedEndpoint)))
    {
        throw new InvalidOperationException(
            $"AutoBalance=shuffle2 should keep first-attempt opens inside the priority subset while it is healthy. observed=[{string.Join(",", observations.Select(static observation => observation.ConnectedEndpoint))}]");
    }

    if (observations.Any(observation => nonPrioritySubset.Contains(observation.ConnectedEndpoint)))
        throw new InvalidOperationException("AutoBalance=shuffle2 unexpectedly used a non-priority seed while the priority subset stayed healthy.");

    if (prioritySubset.Count > 1 && observations.Select(static observation => observation.ConnectedEndpoint).Distinct(StringComparer.Ordinal).Count() < 2)
    {
        throw new InvalidOperationException(
            $"AutoBalance=shuffle2 should shuffle within the priority subset over repeated opens. observed=[{string.Join(",", observations.Select(static observation => observation.ConnectedEndpoint))}]");
    }

    Console.WriteLine("validation-mode=autobalance-shuffle-subset-routing");
}

static async Task AutoBalanceSpecifiedSeedOnlyAsync(Options options)
{
    // 验证 specified：
    // 即使刷新结果里注入了 forged dynamic endpoint，排序时也只能在 seed hosts 范围内轮转。
    var seedRoutes = await LoadSeedRoutesAsync(options);
    if (seedRoutes.Length < 2)
        throw new InvalidOperationException("Specified routing scenario requires at least two seed targets.");

    await using var forgedProxyGroup = new ProxyGroup(options.Targets);
    var overrideCoordinators = new List<CoordinatorMetadata>(seedRoutes.Length);
    foreach (var seedRoute in seedRoutes)
    {
        var originalEndpoint = seedRoute.SeedEndpoint;
        var forgedEndpoint = ParseEndpoint(forgedProxyGroup.GetByIndex(seedRoute.TargetIndex).Endpoint);
        overrideCoordinators.Add(new CoordinatorMetadata(
            seedRoute.NodeName,
            forgedEndpoint,
            forgedEndpoint,
            originalEndpoint,
            originalEndpoint));
    }

    var primarySeed = seedRoutes[0];
    await using var metadataProxy = PgMetadataRewriteProxy.Start(
        primarySeed.SeedEndpoint.Host,
        primarySeed.SeedEndpoint.Port,
        overrideCoordinators);

    var seedTargets = options.Targets.ToArray();
    seedTargets[0] = metadataProxy.Endpoint;
    var connectionString = ConnectionStringUtil.BuildConnectionString(
        seedTargets,
        options.BaseExtra,
        "AutoBalance=specified;RefreshCNIpListTime=30");
    var expectedSeedEndpoints = seedTargets.ToHashSet(StringComparer.Ordinal);
    var forgedEndpoints = Enumerable.Range(0, seedRoutes.Length)
        .Select(index => forgedProxyGroup.GetByIndex(index).Endpoint)
        .ToHashSet(StringComparer.Ordinal);

    Console.WriteLine($"ConnectionString={connectionString}");
    Console.WriteLine($"metadata-proxy={metadataProxy.Endpoint} target={metadataProxy.Target}");
    Console.WriteLine($"expected-seeds={string.Join(",", expectedSeedEndpoints.OrderBy(static x => x, StringComparer.Ordinal))}");
    Console.WriteLine($"forged-dynamic-endpoints={string.Join(",", forgedEndpoints.OrderBy(static x => x, StringComparer.Ordinal))}");

    var observations = await SampleOpenObservationsAsync(connectionString, Math.Max(6, seedTargets.Length * 2));
    DumpObservations(observations);
    Console.WriteLine($"metadata-proxy-seen-sql={string.Join(" || ", metadataProxy.SeenSql)}");

    if (metadataProxy.SeenSql.Length == 0)
        throw new InvalidOperationException("AutoBalance=specified scenario expected to hit pgxc_node refresh, but no refresh SQL was observed.");

    if (observations.Any(observation => !expectedSeedEndpoints.Contains(observation.ConnectedEndpoint)))
    {
        throw new InvalidOperationException(
            $"AutoBalance=specified should stay on seed hosts only. observed=[{string.Join(",", observations.Select(static observation => observation.ConnectedEndpoint))}]");
    }

    if (observations.Any(observation => forgedEndpoints.Contains(observation.ConnectedEndpoint)))
        throw new InvalidOperationException("AutoBalance=specified unexpectedly connected to a forged dynamic endpoint.");

    Console.WriteLine("validation-mode=autobalance-specified-seed-only");
}

static async Task AutoBalanceLeastConnPreservesOrderAsync(Options options)
{
    // 当前 leastconn 还没有真正按活动连接数做挑选，因此现阶段语义是“保留当前候选顺序”。
    // 这个场景就是把这个现状固化为测试。
    if (options.Targets.Length < 2)
        throw new InvalidOperationException("Least-connection scenario requires at least two seed targets.");

    await using var proxyGroup = new ProxyGroup(options.Targets);
    var firstSeed = proxyGroup.GetByIndex(0);
    var secondSeed = proxyGroup.GetByIndex(1);
    var connectionString = proxyGroup.ConnectionString(
        options.BaseExtra,
        "AutoBalance=leastconn;RefreshCNIpListTime=0");

    Console.WriteLine($"ConnectionString={connectionString}");
    Console.WriteLine($"first-seed={firstSeed.Endpoint}");
    Console.WriteLine($"second-seed={secondSeed.Endpoint}");

    // 健康时应始终命中第一个 seed。
    var baselineObservations = await SampleOpenObservationsAsync(connectionString, 4);
    DumpObservations(baselineObservations);
    if (baselineObservations.Any(observation => observation.ConnectedEndpoint != firstSeed.Endpoint))
    {
        throw new InvalidOperationException(
            $"AutoBalance=leastconn should preserve candidate order in the current implementation. expected={firstSeed.Endpoint} observed=[{string.Join(",", baselineObservations.Select(static observation => observation.ConnectedEndpoint))}]");
    }

    // 第一个 seed 故障后，再验证是否按顺序切到第二个 seed。
    await firstSeed.DisableAsync();
    Console.WriteLine($"disabled-first-seed={firstSeed.Endpoint}");

    var failoverObservation = (await SampleOpenObservationsAsync(connectionString, 1)).Single();
    Console.WriteLine($"failover connected-via={failoverObservation.ConnectedEndpoint} node-name={failoverObservation.NodeName} server={failoverObservation.ServerEndpoint}");
    if (failoverObservation.ConnectedEndpoint != secondSeed.Endpoint)
    {
        throw new InvalidOperationException(
            $"After the first seed fails, AutoBalance=leastconn should move to the next seed in order. expected={secondSeed.Endpoint} observed={failoverObservation.ConnectedEndpoint}");
    }

    Console.WriteLine("validation-mode=autobalance-leastconn-preserves-order");
}

static async Task<OpenObservation[]> ObservePriorityAutoBalanceTrueAsync(Options options, bool loadBalanceHosts)
{
    // 同一个 AutoBalance=true 场景，只切换 LoadBalanceHosts 开关，方便直接比较两组观测序列。
    var preferredRoutes = GetPreferredRoutesForPriorityScenario(await LoadSeedRoutesAsync(options), options);
    var connectionString = ConnectionStringUtil.BuildConnectionString(
        options.Targets,
        options.BaseExtra,
        $"PriorityServers={options.PriorityServersForScenario};AutoBalance=true;LoadBalanceHosts={loadBalanceHosts.ToString().ToLowerInvariant()};RefreshCNIpListTime=0");

    Console.WriteLine($"label=autobalance-true-loadbalance-{loadBalanceHosts.ToString().ToLowerInvariant()}");
    Console.WriteLine($"ConnectionString={connectionString}");
    Console.WriteLine($"preferred-seeds={string.Join(",", preferredRoutes.Select(static route => route.Target))}");

    var observations = await SampleOpenObservationsAsync(connectionString, preferredRoutes.Length * 2);
    DumpObservations(observations);

    return observations;
}

static async Task<OpenObservation[]> ObserveAutoBalanceModeAsync(Options options, string autoBalance, int attempts)
{
    // 通用观测辅助：给定一个 AutoBalance 值，多次 Open 后返回实际命中的 endpoint 序列。
    var preferredRoutes = GetPreferredRoutesForPriorityScenario(await LoadSeedRoutesAsync(options), options);
    var connectionString = ConnectionStringUtil.BuildConnectionString(
        options.Targets,
        options.BaseExtra,
        $"PriorityServers={options.PriorityServersForScenario};AutoBalance={autoBalance};LoadBalanceHosts=false;RefreshCNIpListTime=0");

    Console.WriteLine($"label=autobalance-{autoBalance}");
    Console.WriteLine($"ConnectionString={connectionString}");
    Console.WriteLine($"preferred-seeds={string.Join(",", preferredRoutes.Select(static route => route.Target))}");

    var observations = await SampleOpenObservationsAsync(connectionString, attempts);
    DumpObservations(observations);
    return observations;
}

static SeedRoute[] GetPreferredRoutesForPriorityScenario(SeedRoute[] seedRoutes, Options options)
{
    if (options.PriorityServersForScenario <= 0 || options.PriorityServersForScenario >= seedRoutes.Length)
    {
        throw new InvalidOperationException(
            $"Priority routing scenarios require 0 < PriorityServers < host count. priorityServers={options.PriorityServersForScenario} hosts={seedRoutes.Length}");
    }

    return seedRoutes.Take(options.PriorityServersForScenario).ToArray();
}

static SeedRoute[] GetFallbackRoutesForPriorityScenario(SeedRoute[] seedRoutes, Options options)
    => seedRoutes.Skip(options.PriorityServersForScenario).ToArray();

static async Task<OpenObservation[]> SampleOpenObservationsAsync(string connectionString, int attempts)
{
    // 每次都新建一次物理连接，采样“这次 Open 最终命中了哪个 endpoint / node_name”。
    await using var dataSource = new GaussDBDataSourceBuilder(connectionString).BuildMultiHost();
    var observations = new List<OpenObservation>(attempts);
    for (var i = 0; i < attempts; i++)
    {
        await using var conn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);
        var nodeName = await ExecuteScalarTextAsync(conn, "SELECT get_nodename();");
        var serverEndpoint = await ExecuteScalarTextAsync(conn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
        observations.Add(new(i + 1, $"{conn.Host}:{conn.Port}", nodeName, serverEndpoint));
    }

    return observations.ToArray();
}

static void DumpObservations(IReadOnlyList<OpenObservation> observations)
{
    foreach (var observation in observations)
        Console.WriteLine($"open[{observation.Attempt}] connected-via={observation.ConnectedEndpoint} server={observation.ServerEndpoint} node-name={observation.NodeName}");

    Console.WriteLine($"observed-endpoints={string.Join(",", observations.Select(static observation => observation.ConnectedEndpoint).Distinct(StringComparer.Ordinal).OrderBy(static x => x, StringComparer.Ordinal))}");
    Console.WriteLine($"observed-nodes={string.Join(",", observations.Select(static observation => observation.NodeName).Distinct(StringComparer.Ordinal).OrderBy(static x => x, StringComparer.Ordinal))}");
}

static Task InvalidAutoBalanceRejectedAsync(Options options)
{
    // 验证裸数字 AutoBalance 在连接串阶段就会被拒绝。
    var connectionString = ConnectionStringUtil.BuildConnectionString(
        new[] { options.Targets[0] },
        options.BaseExtra,
        "AutoBalance=3");
    Console.WriteLine($"ConnectionString={connectionString}");

    Exception? captured = null;
    try
    {
        _ = new GaussDBConnectionStringBuilder(connectionString);
        throw new InvalidOperationException("AutoBalance=3 unexpectedly succeeded.");
    }
    catch (Exception ex)
    {
        captured = ex;
    }

    if (captured is InvalidOperationException invalidOperationException &&
        invalidOperationException.Message.StartsWith("AutoBalance=3 unexpectedly succeeded", StringComparison.Ordinal))
        throw captured;

    Console.WriteLine($"captured={captured!.GetType().Name}: {captured.Message}");

    var message = captured.Message;
    if (captured.InnerException is not null)
        message += " || " + captured.InnerException.Message;

    if (captured is not ArgumentException ||
        !message.Contains("AutoBalance must use a named routing mode", StringComparison.Ordinal))
    {
        throw new InvalidOperationException(
            $"Expected a named-mode AutoBalance validation error, but captured {captured.GetType().Name}: {captured.Message}",
            captured);
    }

    return Task.CompletedTask;
}

static Task InvalidAutoBalancePriorityBoundRejectedAsync(Options options)
{
    // priorityN 的 N 必须严格小于 seedHostCount。
    var invalidPriority = options.Targets.Length;
    var connectionString = ConnectionStringUtil.BuildConnectionString(
        options.Targets,
        options.BaseExtra,
        $"AutoBalance=priority{invalidPriority}");
    Console.WriteLine($"ConnectionString={connectionString}");

    Exception? captured = null;
    try
    {
        _ = new GaussDBDataSourceBuilder(connectionString).BuildMultiHost();
        throw new InvalidOperationException($"AutoBalance=priority{invalidPriority} unexpectedly succeeded.");
    }
    catch (Exception ex)
    {
        captured = ex;
    }

    if (captured is InvalidOperationException invalidOperationException &&
        invalidOperationException.Message.StartsWith($"AutoBalance=priority{invalidPriority} unexpectedly succeeded", StringComparison.Ordinal))
        throw captured;

    Console.WriteLine($"captured={captured!.GetType().Name}: {captured.Message}");

    if (captured is not ArgumentException ||
        !captured.Message.Contains("AutoBalance priority modes must end with a numeric suffix smaller than the number of seed hosts.", StringComparison.Ordinal))
    {
        throw new InvalidOperationException(
            $"Expected AutoBalance priority bound validation error, but captured {captured.GetType().Name}: {captured.Message}",
            captured);
    }

    return Task.CompletedTask;
}

static Task InvalidAutoBalanceShuffleBoundRejectedAsync(Options options)
{
    // shuffleN 的 N 允许等于 seedHostCount，但不允许超过它。
    var invalidPriority = options.Targets.Length + 1;
    var connectionString = ConnectionStringUtil.BuildConnectionString(
        options.Targets,
        options.BaseExtra,
        $"AutoBalance=shuffle{invalidPriority}");
    Console.WriteLine($"ConnectionString={connectionString}");

    Exception? captured = null;
    try
    {
        _ = new GaussDBDataSourceBuilder(connectionString).BuildMultiHost();
        throw new InvalidOperationException($"AutoBalance=shuffle{invalidPriority} unexpectedly succeeded.");
    }
    catch (Exception ex)
    {
        captured = ex;
    }

    if (captured is InvalidOperationException invalidOperationException &&
        invalidOperationException.Message.StartsWith($"AutoBalance=shuffle{invalidPriority} unexpectedly succeeded", StringComparison.Ordinal))
        throw captured;

    Console.WriteLine($"captured={captured!.GetType().Name}: {captured.Message}");

    if (captured is not ArgumentException ||
        !captured.Message.Contains("AutoBalance shuffle modes must end with a numeric suffix smaller than or equal to the number of seed hosts.", StringComparison.Ordinal))
    {
        throw new InvalidOperationException(
            $"Expected AutoBalance shuffle bound validation error, but captured {captured.GetType().Name}: {captured.Message}",
            captured);
    }

    return Task.CompletedTask;
}

static async Task HostRecheckExpiryReprobesAsync(Options options)
{
    // 第一个代理只拒绝第一次连接，随后恢复；
    // 验证 Offline 冷却到期后，驱动会重新探测并切回这个节点。
    if (options.Targets.Length < 2)
        throw new InvalidOperationException("Host recheck expiry scenario requires at least two seed targets.");

    var seedRoutes = await LoadSeedRoutesAsync(options);
    await using var primaryProxy = RealTcpFaultProxy.Start(
        seedRoutes[0].SeedEndpoint.Host,
        seedRoutes[0].SeedEndpoint.Port,
        initialRejectedConnectionCount: 1);
    await using var fallbackProxy = RealTcpFaultProxy.Start(
        seedRoutes[1].SeedEndpoint.Host,
        seedRoutes[1].SeedEndpoint.Port);

    var connectionString = ConnectionStringUtil.BuildConnectionString(
        new[] { primaryProxy.Endpoint, fallbackProxy.Endpoint },
        options.BaseExtra,
        "HostRecheckSeconds=1");
    Console.WriteLine($"ConnectionString={connectionString}");
    Console.WriteLine($"primary-proxy={primaryProxy.Endpoint}");
    Console.WriteLine($"fallback-proxy={fallbackProxy.Endpoint}");

    await using var dataSource = new GaussDBDataSourceBuilder(connectionString).BuildMultiHost();
    await using (var firstConn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any))
    {
        var firstServer = await ExecuteScalarTextAsync(firstConn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
        Console.WriteLine($"first-open connected-via={firstConn.Host}:{firstConn.Port} server={firstServer}");

        if (firstConn.Port != fallbackProxy.Port)
            throw new InvalidOperationException(
                $"Expected the first open to fall back to {fallbackProxy.Endpoint}, but connected via {firstConn.Host}:{firstConn.Port}.");
    }

    // 等待 HostRecheckSeconds 到期，再发起第二次 Open。
    await Task.Delay(TimeSpan.FromSeconds(1.2));

    await using var secondConn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);
    var secondServer = await ExecuteScalarTextAsync(secondConn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
    Console.WriteLine($"second-open connected-via={secondConn.Host}:{secondConn.Port} server={secondServer}");
    Console.WriteLine($"primary-proxy-connections={primaryProxy.ConnectionCount}");

    if (secondConn.Port != primaryProxy.Port)
        throw new InvalidOperationException(
            $"Expected HostRecheckSeconds expiry to reprobe {primaryProxy.Endpoint}, but connected via {secondConn.Host}:{secondConn.Port}.");
}

static async Task OfflineCacheSkipsImmediateReprobeAsync(Options options)
{
    // 第一个 endpoint 永远立即拒绝，验证在 HostRecheckSeconds 窗口内它会被跳过，不会反复探测。
    var fallbackTarget = ParseEndpoint(options.Targets[0]);
    await using var rejectingProbe = RejectingEndpointProbe.Start();
    await using var fallbackProxy = RealTcpFaultProxy.Start(fallbackTarget.Host, fallbackTarget.Port);
    var connectionString = ConnectionStringUtil.BuildConnectionString(
        new[] { rejectingProbe.Endpoint, fallbackProxy.Endpoint },
        options.BaseExtra,
        "HostRecheckSeconds=60");
    Console.WriteLine($"ConnectionString={connectionString}");
    Console.WriteLine($"rejecting-endpoint={rejectingProbe.Endpoint}");
    Console.WriteLine($"fallback-proxy={fallbackProxy.Endpoint}");

    await using var dataSource = new GaussDBDataSourceBuilder(connectionString).BuildMultiHost();
    await using (var firstConn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any))
    {
        var server = await ExecuteScalarTextAsync(firstConn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
        Console.WriteLine($"first-open connected-via={firstConn.Host}:{firstConn.Port} server={server}");
    }

    var countAfterFirst = rejectingProbe.ConnectionCount;
    await using (var secondConn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any))
    {
        var server = await ExecuteScalarTextAsync(secondConn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
        Console.WriteLine($"second-open connected-via={secondConn.Host}:{secondConn.Port} server={server}");
    }

    var countAfterSecond = rejectingProbe.ConnectionCount;
    Console.WriteLine($"rejecting-connections first={countAfterFirst} second={countAfterSecond}");

    if (countAfterFirst != 1 || countAfterSecond != 1)
        throw new InvalidOperationException(
            $"Expected the Offline endpoint to be skipped before HostRecheckSeconds expiry. observed-first={countAfterFirst} observed-second={countAfterSecond}");
}

static async Task HostRecheckZeroImmediateReprobeAsync(Options options)
{
    // HostRecheckSeconds=0 时，Offline 应立即过期，因此第二次 Open 应再次探测失败节点。
    var fallbackTarget = ParseEndpoint(options.Targets[0]);
    await using var rejectingProbe = RejectingEndpointProbe.Start();
    await using var fallbackProxy = RealTcpFaultProxy.Start(fallbackTarget.Host, fallbackTarget.Port);
    var connectionString = ConnectionStringUtil.BuildConnectionString(
        new[] { rejectingProbe.Endpoint, fallbackProxy.Endpoint },
        options.BaseExtra,
        "HostRecheckSeconds=0");
    Console.WriteLine($"ConnectionString={connectionString}");
    Console.WriteLine($"rejecting-endpoint={rejectingProbe.Endpoint}");
    Console.WriteLine($"fallback-proxy={fallbackProxy.Endpoint}");

    await using var dataSource = new GaussDBDataSourceBuilder(connectionString).BuildMultiHost();
    await using (var firstConn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any))
    {
        var server = await ExecuteScalarTextAsync(firstConn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
        Console.WriteLine($"first-open connected-via={firstConn.Host}:{firstConn.Port} server={server}");
    }

    var countAfterFirst = rejectingProbe.ConnectionCount;
    await using (var secondConn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any))
    {
        var server = await ExecuteScalarTextAsync(secondConn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
        Console.WriteLine($"second-open connected-via={secondConn.Host}:{secondConn.Port} server={server}");
    }

    var countAfterSecond = rejectingProbe.ConnectionCount;
    Console.WriteLine($"rejecting-connections first={countAfterFirst} second={countAfterSecond}");

    if (countAfterFirst != 1 || countAfterSecond < 2)
        throw new InvalidOperationException(
            $"Expected HostRecheckSeconds=0 to allow immediate reprobe. observed-first={countAfterFirst} observed-second={countAfterSecond}");
}

static async Task AllOfflineFallbackRecoveredAsync(Options options)
{
    // 两个 endpoint 的第一次连接都失败，模拟“整轮都被标记为 Offline”。
    // 随后第一个 endpoint 立即恢复，验证同一轮 Open 内的 allowOffline 兜底重扫能把它重新捞回来。
    if (options.Targets.Length < 2)
        throw new InvalidOperationException("All-offline fallback scenario requires at least two seed targets.");

    var seedRoutes = await LoadSeedRoutesAsync(options);
    await using var firstProxy = RealTcpFaultProxy.Start(
        seedRoutes[0].SeedEndpoint.Host,
        seedRoutes[0].SeedEndpoint.Port,
        initialRejectedConnectionCount: 1);
    await using var secondProxy = RealTcpFaultProxy.Start(
        seedRoutes[1].SeedEndpoint.Host,
        seedRoutes[1].SeedEndpoint.Port,
        initialRejectedConnectionCount: 1);

    var connectionString = ConnectionStringUtil.BuildConnectionString(
        new[] { firstProxy.Endpoint, secondProxy.Endpoint },
        options.BaseExtra,
        "HostRecheckSeconds=60");
    Console.WriteLine($"ConnectionString={connectionString}");
    Console.WriteLine($"first-proxy={firstProxy.Endpoint}");
    Console.WriteLine($"second-proxy={secondProxy.Endpoint}");

    await using var conn = new GaussDBConnection(connectionString);
    await conn.OpenAsync();

    var server = await ExecuteScalarTextAsync(conn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
    Console.WriteLine($"connected-via={conn.Host}:{conn.Port} server={server}");
    Console.WriteLine($"first-proxy-connections={firstProxy.ConnectionCount}");
    Console.WriteLine($"second-proxy-connections={secondProxy.ConnectionCount}");

    if (firstProxy.ConnectionCount < 2)
        throw new InvalidOperationException(
            $"Expected the all-offline fallback to retry the first endpoint within the same Open. observed={firstProxy.ConnectionCount}");

    if (conn.Port != firstProxy.Port)
        throw new InvalidOperationException(
            $"Expected fallback allowOffline pass to reconnect to {firstProxy.Endpoint}, but connected via {conn.Host}:{conn.Port}.");
}

static async Task ProxyDisconnectNoReplayAsync(Options options)
{
    // 验证命令执行阶段如果链路被代理强行断开，驱动不会透明重放当前命令。
    // 验证代理断开时命令不会被透明重放，调用方应收到失败而不是悄悄成功。
    await using var proxyGroup = new ProxyGroup(options.Targets);
    var connectionString = proxyGroup.ConnectionString(options.BaseExtra, "PriorityServers=2;AutoReconnect=true;MaxReconnects=3;RefreshCNIpListTime=1;AutoBalance=Shuffle");
    Console.WriteLine($"ConnectionString={connectionString}");

    await using var conn = new GaussDBConnection(connectionString);
    await conn.OpenAsync();

    var currentProxy = proxyGroup.FindByPort(conn.Port)
                      ?? throw new InvalidOperationException($"No proxy found for port {conn.Port}");
    Console.WriteLine($"initial-proxy={currentProxy.Endpoint} target={currentProxy.Target}");

    // 先发一个长时间命令，让连接保持在“执行中”状态，再中途掐断代理。
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
    // 验证显式事务内发生 AdminShutdown 时，驱动不会重放命令或偷偷换 backend。
    // 显式事务内不允许自动重放，否则会破坏事务语义。
    var connectionString = ConnectionStringUtil.BuildConnectionString(options.Targets, options.BaseExtra, "PriorityServers=2;AutoReconnect=true;MaxReconnects=3");
    Console.WriteLine($"ConnectionString={connectionString}");

    await using var conn = new GaussDBConnection(connectionString);
    await conn.OpenAsync();
    await using var tx = await conn.BeginTransactionAsync();

    var initialHost = conn.Host;
    var initialPid = await ExecuteScalarLongAsync(conn, "SELECT pg_backend_pid();", tx);
    Console.WriteLine($"initial-host={initialHost} initial-pid={initialPid}");

    // 保持事务内命令执行中，然后用控制连接显式 pg_terminate_backend。
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

static async Task CopyExportDisconnectNoReplayAsync(Options options)
{
    // 验证 COPY 导出过程中断链时，不会透明重放 COPY。
    await using var proxyGroup = new ProxyGroup(options.Targets);
    var connectionString = proxyGroup.ConnectionString(options.BaseExtra, "PriorityServers=2;AutoReconnect=true;MaxReconnects=3");
    Console.WriteLine($"ConnectionString={connectionString}");

    await using var conn = new GaussDBConnection(connectionString);
    await conn.OpenAsync();

    var currentProxy = proxyGroup.FindByPort(conn.Port)
                      ?? throw new InvalidOperationException($"No proxy found for port {conn.Port}");
    Console.WriteLine($"initial-proxy={currentProxy.Endpoint} target={currentProxy.Target}");

    TextReader? reader = null;
    Exception? captured = null;
    try
    {
        // 先把 COPY 流拉起并读到第一块数据，确保后续断链发生在 COPY 执行过程中。
        reader = await conn.BeginTextExportAsync(
            "COPY (SELECT repeat('x', 8192) FROM generate_series(1, 100000)) TO STDOUT");
        var buffer = new char[8192];
        var firstRead = await reader.ReadAsync(buffer, 0, buffer.Length);
        Console.WriteLine($"copy-first-chunk={firstRead}");
        if (firstRead <= 0)
            throw new InvalidOperationException("COPY export did not produce the first chunk.");

        await currentProxy.DisableAsync();
        Console.WriteLine($"disabled-proxy={currentProxy.Endpoint}");

        while (true)
        {
            var read = await reader.ReadAsync(buffer, 0, buffer.Length);
            if (read == 0)
                break;
        }

        throw new InvalidOperationException("COPY export unexpectedly completed after proxy disconnect.");
    }
    catch (Exception ex)
    {
        captured = ex;
    }
    finally
    {
        if (reader is not null)
        {
            try
            {
                reader.Dispose();
            }
            catch (Exception ex) when (captured is not null)
            {
                Console.WriteLine($"dispose-captured={ex.GetType().Name}: {ex.Message}");
                if (captured is InvalidOperationException pendingInvalidOperation &&
                    pendingInvalidOperation.Message.StartsWith("COPY export unexpectedly completed", StringComparison.Ordinal))
                    captured = ex;
            }
        }
    }

    if (captured is InvalidOperationException invalidOperationException &&
        invalidOperationException.Message.StartsWith("COPY export unexpectedly completed", StringComparison.Ordinal))
        throw captured;

    Console.WriteLine($"captured={captured!.GetType().Name}: {captured.Message}");
}

static async Task ActiveReaderNoReplayAsync(Options options)
{
    // 验证活跃 reader 正在消费大字段时断链，不会被透明重连后继续读。
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
        // 先读到第一块大字段，确保 reader 已进入流式消费阶段。
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
    // 验证第一个 reader 仍在活动期间，第二个命令必须被拒绝，不能通过重连绕过去。
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

        // 保持第一个 reader 仍然未结束，再尝试发第二条命令。
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
    // 验证命令超时不是故障转移信号，不应触发重连或偷偷换 backend。
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

static async Task SeedBindingRebindScenarioAsync(Options options, bool usingEip)
{
    // 基础 rebind 场景：
    // dynamic endpoint 可用时优先用 dynamic；失效后回退 seed；恢复后再切回 dynamic。
    await ExecuteDynamicEndpointRebindAsync(options, usingEip, verifyRepeatedPostRecoveryOpen: false);
}

static async Task SeedBindingRebindStateCheckAsync(Options options)
{
    // 在基础 rebind 场景上再多做一次 Open，
    // 确认动态地址恢复后，后续连接会稳定维持在恢复后的 dynamic endpoint。
    await ExecuteDynamicEndpointRebindAsync(options, usingEip: true, verifyRepeatedPostRecoveryOpen: true);
}

static async Task SingleClusterEnhancedHaWithoutPriorityServersAsync(Options options)
{
    // 验证未配置 PriorityServers 时，只要显式打开 AutoBalance + Refresh，
    // 驱动仍会走单簇增强 HA 路径，而不是退回纯 seed 静态建连。
    var seedRoutes = await LoadSeedRoutesAsync(options);
    var seedRoute = seedRoutes[0];
    var coordinatorMetadata = await LoadCoordinatorMetadataByNodeNameAsync(options, seedRoute.Target);
    var seedCoordinator = coordinatorMetadata[seedRoute.NodeName];

    await using var forgedPreferredProxy = RealTcpFaultProxy.Start(seedRoute.SeedEndpoint.Host, seedRoute.SeedEndpoint.Port);
    var forgedPreferredEndpoint = ParseEndpoint(forgedPreferredProxy.Endpoint);
    var overrideCoordinator = new CoordinatorMetadata(
        seedRoute.NodeName,
        forgedPreferredEndpoint,
        forgedPreferredEndpoint,
        seedCoordinator.HostEndpoint,
        seedCoordinator.EipEndpoint);

    await using var metadataProxy = PgMetadataRewriteProxy.Start(
        seedRoute.SeedEndpoint.Host,
        seedRoute.SeedEndpoint.Port,
        new[] { overrideCoordinator });
    var connectionString = ConnectionStringUtil.BuildConnectionString(
        new[] { metadataProxy.Endpoint },
        options.BaseExtra,
        "AutoBalance=roundrobin;RefreshCNIpListTime=30");

    Console.WriteLine($"seed-target={seedRoute.Target}");
    Console.WriteLine($"seed-node-name={seedRoute.NodeName}");
    Console.WriteLine($"metadata-proxy={metadataProxy.Endpoint} target={metadataProxy.Target}");
    Console.WriteLine($"forged-preferred-endpoint={forgedPreferredEndpoint}");
    Console.WriteLine($"ConnectionString={connectionString}");

    await using var dataSource = new GaussDBDataSourceBuilder(connectionString).BuildMultiHost();
    await using var conn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);

    // 这里的正确结果是：仍然通过 seed proxy 建连，并且 metadata proxy 看不到任何刷新 SQL。
    var connectedNodeName = await ExecuteScalarTextAsync(conn, "SELECT get_nodename();");
    var serverEndpoint = await ExecuteScalarTextAsync(conn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
    var connectedEndpoint = new Endpoint(conn.Host!, conn.Port);
    Console.WriteLine($"connected-via={connectedEndpoint} server={serverEndpoint} node-name={connectedNodeName}");
    Console.WriteLine($"metadata-proxy-seen-sql={string.Join(" || ", metadataProxy.SeenSql)}");

    if (connectedNodeName != seedRoute.NodeName)
        throw new InvalidOperationException(
            $"Expected single-cluster enhanced HA to stay bound to node '{seedRoute.NodeName}', but connected to '{connectedNodeName}'.");

    if (connectedEndpoint.ToString() != forgedPreferredEndpoint.ToString())
    {
        throw new InvalidOperationException(
            $"Expected single-cluster enhanced HA without PriorityServers to use forged dynamic endpoint {forgedPreferredEndpoint}, but connected via {connectedEndpoint}.");
    }

    if (connectedEndpoint.ToString() == metadataProxy.Endpoint)
        throw new InvalidOperationException("Single-cluster enhanced HA unexpectedly stayed on the seed endpoint instead of using the discovered endpoint.");

    Console.WriteLine("validation-mode=single-cluster-enhanced-ha-without-priorityservers");
}

static async Task ExecuteDynamicEndpointRebindAsync(Options options, bool usingEip, bool verifyRepeatedPostRecoveryOpen)
{
    // 这个辅助函数统一覆盖两类场景：
    // 1. UsingEip=true/false 时 dynamic endpoint 的首选列是否正确；
    // 2. dynamic 失效 -> 回退 seed -> dynamic 恢复后的回切路径是否正确。
    var seedRoutes = await LoadSeedRoutesAsync(options);
    var seedRoute = seedRoutes[0];
    var coordinatorMetadata = await LoadCoordinatorMetadataByNodeNameAsync(options, seedRoute.Target);
    var seedCoordinator = coordinatorMetadata[seedRoute.NodeName];

    await using var dynamicProxy = RealTcpFaultProxy.Start(seedRoute.SeedEndpoint.Host, seedRoute.SeedEndpoint.Port);
    var dynamicEndpoint = ParseEndpoint(dynamicProxy.Endpoint);
    var rewrittenCoordinator = usingEip
        ? new CoordinatorMetadata(
            seedRoute.NodeName,
            seedCoordinator.HostEndpoint,
            dynamicEndpoint,
            seedCoordinator.HostEndpoint,
            seedCoordinator.EipEndpoint)
        : new CoordinatorMetadata(
            seedRoute.NodeName,
            dynamicEndpoint,
            seedCoordinator.EipEndpoint,
            seedCoordinator.HostEndpoint,
            seedCoordinator.EipEndpoint);

    await using var metadataProxy = PgMetadataRewriteProxy.Start(
        seedRoute.SeedEndpoint.Host,
        seedRoute.SeedEndpoint.Port,
        new[] { rewrittenCoordinator });

    var scenarioExtra =
        $"AutoBalance=roundrobin;RefreshCNIpListTime={options.RefreshSecondsForScenario};HostRecheckSeconds=1;AutoReconnect=false;Application Name=seed-binding-rebind-{(usingEip ? "eip" : "inner")}-{Guid.NewGuid():N}";
    var connectionString = ConnectionStringUtil.BuildConnectionString(
        new[] { metadataProxy.Endpoint },
        ApplyUsingEipToBaseExtra(options.BaseExtra, usingEip),
        scenarioExtra);

    Console.WriteLine($"ConnectionString={connectionString}");
    Console.WriteLine($"metadata-proxy={metadataProxy.Endpoint} target={metadataProxy.Target}");
    Console.WriteLine($"dynamic-endpoint={dynamicEndpoint} target-node={seedRoute.NodeName} using-eip={usingEip}");

    await using var dataSource = new GaussDBDataSourceBuilder(connectionString).BuildMultiHost();

    // step-1: dynamic endpoint 可达，第一次 Open 应优先命中 dynamic。
    var step1 = await OpenObservationFromDataSourceAsync(dataSource, attempt: 1);
    Console.WriteLine($"step-1 connected={step1.ConnectedEndpoint} node={step1.NodeName} server={step1.ServerEndpoint}");
    Console.WriteLine($"step-1 metadata-proxy-rewritten-rows={metadataProxy.RewrittenRowCount}");
    Console.WriteLine($"step-1 metadata-proxy-seen-sql={string.Join(" || ", metadataProxy.SeenSql)}");
    if (step1.ConnectedEndpoint != dynamicEndpoint.ToString())
        throw new InvalidOperationException($"Expected step-1 to prefer dynamic endpoint {dynamicEndpoint}, actual={step1.ConnectedEndpoint}.");

    // step-2: 把 dynamic endpoint 暂停掉，等待刷新窗口过期后再次 Open，应回退到 seed。
    await dynamicProxy.RejectConnectionsAsync();
    Console.WriteLine($"step-2 paused dynamic endpoint={dynamicEndpoint}");
    await Task.Delay(TimeSpan.FromSeconds(options.RefreshSecondsForScenario + 1));

    var step2 = await OpenObservationFromDataSourceAsync(dataSource, attempt: 2);
    Console.WriteLine($"step-2 connected={step2.ConnectedEndpoint} node={step2.NodeName} server={step2.ServerEndpoint}");
    if (step2.ConnectedEndpoint != metadataProxy.Endpoint)
        throw new InvalidOperationException($"Expected step-2 to fall back to metadata seed {metadataProxy.Endpoint}, actual={step2.ConnectedEndpoint}.");

    // step-3: 恢复 dynamic endpoint，再等一个刷新窗口，新的 Open 应重新切回 dynamic。
    await dynamicProxy.ResumeAsync();
    Console.WriteLine($"step-3 resumed dynamic endpoint={dynamicEndpoint}");
    await Task.Delay(TimeSpan.FromSeconds(options.RefreshSecondsForScenario + 1));

    var step3 = await OpenObservationFromDataSourceAsync(dataSource, attempt: 3);
    Console.WriteLine($"step-3 connected={step3.ConnectedEndpoint} node={step3.NodeName} server={step3.ServerEndpoint}");
    if (step3.ConnectedEndpoint != dynamicEndpoint.ToString())
        throw new InvalidOperationException($"Expected step-3 to switch back to dynamic endpoint {dynamicEndpoint}, actual={step3.ConnectedEndpoint}.");

    if (verifyRepeatedPostRecoveryOpen)
    {
        // step-4: 再补一次 Open，验证恢复后的命中结果不是偶发，而是可持续稳定复用。
        var step4 = await OpenObservationFromDataSourceAsync(dataSource, attempt: 4);
        Console.WriteLine($"step-4 connected={step4.ConnectedEndpoint} node={step4.NodeName} server={step4.ServerEndpoint}");
        if (step4.ConnectedEndpoint != dynamicEndpoint.ToString())
            throw new InvalidOperationException($"Expected step-4 to remain on recovered dynamic endpoint {dynamicEndpoint}, actual={step4.ConnectedEndpoint}.");
    }
}

static async Task SqlErrorNoReconnectAsync(Options options)
{
    // 验证普通 SQL 语义错误不是连接级故障，不应触发重连或切换 endpoint。
    // 普通 SQL 语义错误不是连接级故障，不应触发自动重连或更换 backend。
    var connectionString = ConnectionStringUtil.BuildConnectionString(options.Targets, options.BaseExtra, "PriorityServers=2;AutoReconnect=true;MaxReconnects=3");
    Console.WriteLine($"ConnectionString={connectionString}");

    await using var conn = new GaussDBConnection(connectionString);
    await conn.OpenAsync();

    var initialPid = await ExecuteScalarLongAsync(conn, "SELECT pg_backend_pid();");
    var initialEndpoint = new Endpoint(conn.Host!, conn.Port);
    Console.WriteLine($"initial-endpoint={initialEndpoint} initial-pid={initialPid}");

    Exception? captured = null;
    try
    {
        var result = await ExecuteScalarTextAsync(conn, "SELECT * FROM __gaussdb_codex_missing_table_for_sql_error_no_reconnect__;");
        throw new InvalidOperationException($"Semantic SQL error unexpectedly succeeded with result {result}");
    }
    catch (Exception ex)
    {
        captured = ex;
    }

    if (captured is InvalidOperationException invalidOperationException &&
        invalidOperationException.Message.StartsWith("Semantic SQL error unexpectedly succeeded", StringComparison.Ordinal))
        throw captured;

    var afterPid = await ExecuteScalarLongAsync(conn, "SELECT pg_backend_pid();");
    var afterEndpoint = new Endpoint(conn.Host!, conn.Port);
    Console.WriteLine($"captured={captured!.GetType().Name}: {captured.Message}");
    Console.WriteLine($"after-error endpoint={afterEndpoint} pid={afterPid}");

    if (afterPid != initialPid)
        throw new InvalidOperationException("Semantic SQL error unexpectedly reconnected to a new backend.");

    if (afterEndpoint.ToString() != initialEndpoint.ToString())
        throw new InvalidOperationException("Semantic SQL error unexpectedly switched endpoints.");
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

static async Task<OpenObservation> OpenObservationFromDataSourceAsync(GaussDBMultiHostDataSource dataSource, int attempt)
{
    await using var conn = await dataSource.OpenConnectionAsync(TargetSessionAttributes.Any);
    var nodeName = await ExecuteScalarTextAsync(conn, "SELECT get_nodename();");
    var serverEndpoint = await ExecuteScalarTextAsync(conn, "SELECT inet_server_addr()::text || ':' || inet_server_port()::text;");
    return new(attempt, $"{conn.Host}:{conn.Port}", nodeName, serverEndpoint);
}

static async Task<Dictionary<string, CoordinatorMetadata>> LoadCoordinatorMetadataByNodeNameAsync(Options options, string target)
{
    var connectionString = ConnectionStringUtil.BuildConnectionString(new[] { target }, options.BaseExtra, string.Empty);
    await using var conn = new GaussDBConnection(connectionString);
    await conn.OpenAsync();
    return (await LoadActiveCoordinatorsAsync(conn))
        .ToDictionary(static coordinator => coordinator.NodeName, StringComparer.Ordinal);
}

static string ApplyUsingEipToBaseExtra(string baseExtra, bool usingEip)
{
    var parts = baseExtra
        .Split(';', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
        .Where(static part => !part.StartsWith("UsingEip=", StringComparison.OrdinalIgnoreCase))
        .ToList();
    parts.Add($"UsingEip={(usingEip ? "true" : "false")}");
    return string.Join(';', parts);
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

sealed record CoordinatorMetadata(
    string NodeName,
    Endpoint HostEndpoint,
    Endpoint EipEndpoint,
    Endpoint? OriginalHostEndpoint = null,
    Endpoint? OriginalEipEndpoint = null)
{
    internal Endpoint GetPreferredEndpoint(bool usingEip)
        => usingEip ? EipEndpoint : HostEndpoint;

    internal Endpoint GetMatchEndpoint(bool usingEip)
        => usingEip
            ? OriginalEipEndpoint ?? EipEndpoint
            : OriginalHostEndpoint ?? HostEndpoint;
}

sealed record SeedRoute(int TargetIndex, string Target, Endpoint SeedEndpoint, string NodeName);

sealed record SeedProxyRoute(SeedRoute SeedRoute, Endpoint ProxyEndpoint);

sealed record OpenObservation(int Attempt, string ConnectedEndpoint, string NodeName, string ServerEndpoint);

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
    internal const string PgxcNodeRefreshCompactHost =
        "select node_host,node_port from pgxc_node where node_type='C' and nodeis_active = true order by node_host;";
    internal const string PgxcNodeRefreshCompactEip =
        "select node_host1,node_port1 from pgxc_node where node_type='C' and nodeis_active = true order by node_host1;";
}

sealed record Options(
    string Mode,
    string[] Targets,
    string BaseExtra,
    TimeSpan FailDelay,
    int BindBlockTargetIndex,
    int PriorityServersForScenario,
    int RefreshSecondsForScenario)
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
        var bindBlockTargetIndex = int.Parse(GetValue(values, "bind-block-target-index", "REAL_GAUSS_BIND_BLOCK_TARGET_INDEX", "2"));
        var priorityServersForScenario = int.Parse(GetValue(values, "priority-servers", "REAL_GAUSS_PRIORITY_SERVERS", "2"));
        var refreshSecondsForScenario = int.Parse(GetValue(values, "refresh-seconds", "REAL_GAUSS_REFRESH_SECONDS", "1"));

        return new(
            mode,
            targets,
            baseExtra,
            TimeSpan.FromMilliseconds(failDelayMs),
            bindBlockTargetIndex,
            priorityServersForScenario,
            refreshSecondsForScenario);
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

    internal string DescribeEndpoint(string endpoint)
    {
        var proxy = _proxies.FirstOrDefault(proxy => string.Equals(proxy.Endpoint, endpoint, StringComparison.OrdinalIgnoreCase));
        return proxy is null ? "direct/unknown" : $"proxy->{proxy.Target}";
    }

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
    enum RewriteMode
    {
        None,
        FullMetadata,
        CompactHost,
        CompactEip
    }

    readonly TcpListener _listener;
    readonly CancellationTokenSource _shutdownCts = new();
    readonly ConcurrentDictionary<int, RewriteConnectionPair> _connections = new();
    readonly ConcurrentQueue<string> _seenSql = new();
    readonly Task _acceptLoopTask;
    readonly string _targetHost;
    readonly int _targetPort;
    readonly IReadOnlyDictionary<string, CoordinatorMetadata> _overrides;
    readonly Func<string, bool>? _shouldAbortSql;
    int _nextConnectionId;
    bool _disabled;
    int _rewrittenRowCount;

    internal string Endpoint => $"{IPAddress.Loopback}:{Port}";
    internal string Target => $"{_targetHost}:{_targetPort}";
    internal int Port { get; }
    internal string[] SeenSql => _seenSql.ToArray();
    internal int RewrittenRowCount => _rewrittenRowCount;

    PgMetadataRewriteProxy(
        string targetHost,
        int targetPort,
        IReadOnlyDictionary<string, CoordinatorMetadata> overrides,
        Func<string, bool>? shouldAbortSql = null)
    {
        _targetHost = targetHost;
        _targetPort = targetPort;
        _overrides = overrides;
        _shouldAbortSql = shouldAbortSql;
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

    internal static PgMetadataRewriteProxy StartRejectingRefresh(string targetHost, int targetPort)
        => new(
            targetHost,
            targetPort,
            new Dictionary<string, CoordinatorMetadata>(StringComparer.Ordinal),
            static sql => IsRefreshSql(sql));

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
                _shouldAbortSql,
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

    static bool IsRefreshSql(string sql)
    {
        var normalized = NormalizeSql(sql);
        return string.Equals(normalized, NormalizeSql(SqlText.PgxcNodeRefresh), StringComparison.Ordinal) ||
               string.Equals(normalized, NormalizeSql(SqlText.PgxcNodeRefreshCompactHost), StringComparison.Ordinal) ||
               string.Equals(normalized, NormalizeSql(SqlText.PgxcNodeRefreshCompactEip), StringComparison.Ordinal);

        static string NormalizeSql(string value)
            => string.Join(" ", value
                .Trim()
                .TrimEnd(';')
                .Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));
    }

    sealed class RewriteConnectionPair(
        TcpClient client,
        TcpClient server,
        IReadOnlyDictionary<string, CoordinatorMetadata> overrides,
        Func<string, bool>? shouldAbortSql,
        Action<string> recordSql,
        Action recordRewrite)
    {
        readonly TcpClient _client = client;
        readonly TcpClient _server = server;
        readonly IReadOnlyDictionary<string, CoordinatorMetadata> _overrides = overrides;
        readonly Func<string, bool>? _shouldAbortSql = shouldAbortSql;
        readonly Action<string> _recordSql = recordSql;
        readonly Action _recordRewrite = recordRewrite;
        volatile RewriteMode _rewriteMode;

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
                if (TryGetFrontendSql(message, out var sql))
                {
                    if (_shouldAbortSql is not null && _shouldAbortSql(sql))
                    {
                        _recordSql(sql);
                        Close();
                        return;
                    }

                    if (TryGetRewriteMode(sql, out var rewriteMode))
                    {
                        _recordSql(sql);
                        _rewriteMode = rewriteMode;
                    }
                }

                await destination.WriteAsync(message, cancellationToken).ConfigureAwait(false);
                await destination.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        async Task PumpServerToClientAsync(NetworkStream source, NetworkStream destination, CancellationToken cancellationToken)
        {
            while (await TryReadTypedMessageAsync(source, cancellationToken).ConfigureAwait(false) is { } message)
            {
                if (_rewriteMode != RewriteMode.None && message[0] == (byte)'D')
                {
                    var rewritten = RewritePgxcNodeDataRow(message, _overrides, _rewriteMode);
                    if (rewritten.Changed)
                    {
                        message = rewritten.Message;
                        _recordRewrite();
                    }
                }

                await destination.WriteAsync(message, cancellationToken).ConfigureAwait(false);
                await destination.FlushAsync(cancellationToken).ConfigureAwait(false);

                if (_rewriteMode != RewriteMode.None && message[0] == (byte)'Z')
                    _rewriteMode = RewriteMode.None;
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

        static bool TryGetRewriteMode(string sql, out RewriteMode rewriteMode)
        {
            var normalized = NormalizeSql(sql);
            if (string.Equals(normalized, NormalizeSql(SqlText.PgxcNodeRefresh), StringComparison.Ordinal))
            {
                rewriteMode = RewriteMode.FullMetadata;
                return true;
            }

            if (string.Equals(normalized, NormalizeSql(SqlText.PgxcNodeRefreshCompactHost), StringComparison.Ordinal))
            {
                rewriteMode = RewriteMode.CompactHost;
                return true;
            }

            if (string.Equals(normalized, NormalizeSql(SqlText.PgxcNodeRefreshCompactEip), StringComparison.Ordinal))
            {
                rewriteMode = RewriteMode.CompactEip;
                return true;
            }

            rewriteMode = RewriteMode.None;
            return false;
        }

        static (byte[] Message, bool Changed) RewritePgxcNodeDataRow(
            byte[] message,
            IReadOnlyDictionary<string, CoordinatorMetadata> overrides,
            RewriteMode rewriteMode)
        {
            var payload = message.AsSpan(5);
            var fieldCount = BinaryPrimitives.ReadInt16BigEndian(payload[..2]);

            var originalValues = new byte[fieldCount][];
            var offset = 2;
            for (var i = 0; i < fieldCount; i++)
            {
                var fieldLength = BinaryPrimitives.ReadInt32BigEndian(payload.Slice(offset, 4));
                offset += 4;
                if (fieldLength < 0)
                    return (message, false);

                originalValues[i] = payload.Slice(offset, fieldLength).ToArray();
                offset += fieldLength;
            }

            return rewriteMode switch
            {
                RewriteMode.FullMetadata => RewriteFullMetadataRow(message, originalValues, overrides),
                RewriteMode.CompactHost => RewriteCompactMetadataRow(message, originalValues, overrides, usingEip: false),
                RewriteMode.CompactEip => RewriteCompactMetadataRow(message, originalValues, overrides, usingEip: true),
                _ => (message, false)
            };
        }

        static (byte[] Message, bool Changed) RewriteFullMetadataRow(
            byte[] originalMessage,
            byte[][] originalValues,
            IReadOnlyDictionary<string, CoordinatorMetadata> overrides)
        {
            if (originalValues.Length != 5)
                return (originalMessage, false);

            var nodeName = Encoding.UTF8.GetString(originalValues[0]);
            if (!overrides.TryGetValue(nodeName, out var replacement))
                return (originalMessage, false);

            var rewrittenValues = new byte[5][];
            rewrittenValues[0] = originalValues[0];
            rewrittenValues[1] = Encoding.ASCII.GetBytes(replacement.HostEndpoint.Host);
            rewrittenValues[2] = BuildPortBytes(replacement.HostEndpoint.Port, originalValues[2]);
            rewrittenValues[3] = Encoding.ASCII.GetBytes(replacement.EipEndpoint.Host);
            rewrittenValues[4] = BuildPortBytes(replacement.EipEndpoint.Port, originalValues[4]);
            return (BuildDataRowMessage(rewrittenValues), true);
        }

        static (byte[] Message, bool Changed) RewriteCompactMetadataRow(
            byte[] originalMessage,
            byte[][] originalValues,
            IReadOnlyDictionary<string, CoordinatorMetadata> overrides,
            bool usingEip)
        {
            if (originalValues.Length != 2)
                return (originalMessage, false);

            var originalEndpoint = new Endpoint(
                Encoding.UTF8.GetString(originalValues[0]),
                ParsePortBytes(originalValues[1]));
            var replacement = overrides.Values.FirstOrDefault(candidate =>
                EndpointEquals(candidate.GetMatchEndpoint(usingEip), originalEndpoint));
            if (replacement is null)
                return (originalMessage, false);

            var rewrittenEndpoint = replacement.GetPreferredEndpoint(usingEip);
            var rewrittenValues = new byte[2][];
            rewrittenValues[0] = Encoding.ASCII.GetBytes(rewrittenEndpoint.Host);
            rewrittenValues[1] = BuildPortBytes(rewrittenEndpoint.Port, originalValues[1]);
            return (BuildDataRowMessage(rewrittenValues), true);
        }

        static bool EndpointEquals(Endpoint x, Endpoint y)
            => string.Equals(x.Host, y.Host, StringComparison.OrdinalIgnoreCase) && x.Port == y.Port;

        static int ParsePortBytes(byte[] value)
        {
            if (value.Length == 4)
                return BinaryPrimitives.ReadInt32BigEndian(value);

            return int.Parse(Encoding.ASCII.GetString(value), CultureInfo.InvariantCulture);
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
    int _connectionCount;
    int _remainingInitialRejectedConnections;
    bool _disabled;
    volatile bool _rejectNewConnections;

    internal string Endpoint => $"{IPAddress.Loopback}:{Port}";
    internal string Target => $"{_targetHost}:{_targetPort}";
    internal int Port { get; }
    internal int ConnectionCount => Volatile.Read(ref _connectionCount);

    RealTcpFaultProxy(string targetHost, int targetPort, int initialRejectedConnectionCount)
    {
        _targetHost = targetHost;
        _targetPort = targetPort;
        _remainingInitialRejectedConnections = initialRejectedConnectionCount;
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _acceptLoopTask = RunAcceptLoopAsync();
    }

    internal static RealTcpFaultProxy Start(string targetHost, int targetPort, int initialRejectedConnectionCount = 0)
        => new(targetHost, targetPort, initialRejectedConnectionCount);

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

    internal Task RejectConnectionsAsync()
    {
        _rejectNewConnections = true;
        DisconnectExistingConnections();
        return Task.CompletedTask;
    }

    internal Task ResumeAsync()
    {
        if (_disabled)
            throw new InvalidOperationException("Proxy has already been disabled permanently.");

        _rejectNewConnections = false;
        return Task.CompletedTask;
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

            Interlocked.Increment(ref _connectionCount);
            if (_rejectNewConnections || TryConsumeInitialRejectedConnection())
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

    bool TryConsumeInitialRejectedConnection()
    {
        while (true)
        {
            var remaining = Volatile.Read(ref _remainingInitialRejectedConnections);
            if (remaining <= 0)
                return false;

            if (Interlocked.CompareExchange(ref _remainingInitialRejectedConnections, remaining - 1, remaining) == remaining)
                return true;
        }
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


sealed class RejectingEndpointProbe : IAsyncDisposable
{
    readonly TcpListener _listener;
    readonly CancellationTokenSource _shutdownCts = new();
    readonly Task _acceptLoopTask;
    volatile bool _disabled;
    int _connectionCount;

    internal string Endpoint => $"{IPAddress.Loopback}:{Port}";
    internal int Port { get; }
    internal int ConnectionCount => Volatile.Read(ref _connectionCount);

    RejectingEndpointProbe()
    {
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _acceptLoopTask = RunAcceptLoopAsync();
    }

    internal static RejectingEndpointProbe Start()
        => new();

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

            Interlocked.Increment(ref _connectionCount);
            Abort(client);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disabled)
            return;

        _disabled = true;
        _shutdownCts.Cancel();
        _listener.Stop();

        try
        {
            await _acceptLoopTask.ConfigureAwait(false);
        }
        catch
        {
        }

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
}
