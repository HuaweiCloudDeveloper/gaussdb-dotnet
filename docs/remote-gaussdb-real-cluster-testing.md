# Remote GaussDB Real Cluster / HA Scenario Testing

This document describes how to run the real distributed / HA scenario runner against a
reachable GaussDB cluster from a local development machine.

Unlike the standard remote single-node product profile, these scenarios target multi-CN
routing, priority-cluster failover, coordinator discovery, host recheck, dynamic endpoint
rebind, and reconnect-related behavior implemented in
`test/GaussDB.RealClusterScenarios/Program.cs`.

Do not run these scenarios against a shared or important production cluster. Several
scenarios intentionally drop connections, reject new connections through local proxies, or
terminate the current backend session to validate reconnect and failover behavior.

## Prerequisites

- A reachable GaussDB distributed cluster with multiple CN endpoints.
- A test login user that can connect to every target CN listed in the test input.
- Permission for the test user to execute the statements required by the scenarios. Some
  scenarios use `SELECT get_nodename()`, query `pgxc_node`, or terminate the current test
  backend through a control connection.
- .NET 8 SDK/runtime on the client machine.
- Local loopback networking available on the client machine. The scenario runner uses local
  TCP proxies to simulate disconnects, connection refusal, and endpoint remapping.

## Scenario Runner

The real-cluster scenario runner is the console project below:

- `test/GaussDB.RealClusterScenarios`

List all supported scenarios:

```powershell
dotnet run --project .\test\GaussDB.RealClusterScenarios -- list
```

Run the entire real-cluster scenario matrix:

```powershell
dotnet run --project .\test\GaussDB.RealClusterScenarios -- matrix
```

Run one specific scenario:

```powershell
dotnet run --project .\test\GaussDB.RealClusterScenarios -- open-failover
```

## Required Environment Variables

The runner reads its inputs from command-line options or environment variables. For local
repeated runs, environment variables are the simplest path.

Set the CN targets and the base connection string fields:

```powershell
$env:REAL_GAUSS_TARGETS = "cn1.example.com:8000,cn2.example.com:8000,cn3.example.com:8000"
$env:REAL_GAUSS_EXTRA = "Database=postgres;Username=root;Password=<password>;Pooling=false;Timeout=5;Command Timeout=5;Multiplexing=false"
```

Optional tuning inputs:

```powershell
$env:REAL_GAUSS_PRIORITY_SERVERS = "2"
$env:REAL_GAUSS_REFRESH_SECONDS = "1"
$env:REAL_GAUSS_FAIL_DELAY_MS = "1000"
$env:REAL_GAUSS_BIND_BLOCK_TARGET_INDEX = "2"
```

Variable meanings:

- `REAL_GAUSS_TARGETS`: comma-separated CN seed endpoints used as the scenario input host
  list.
- `REAL_GAUSS_EXTRA`: all connection string fields except `Host=...`.
- `REAL_GAUSS_PRIORITY_SERVERS`: how many leading hosts should be treated as the preferred
  cluster in priority-routing scenarios. Default is `2`.
- `REAL_GAUSS_REFRESH_SECONDS`: refresh interval used by discovery / rebind scenarios.
  Default is `1`.
- `REAL_GAUSS_FAIL_DELAY_MS`: delay used by specific timed failover scenarios. Default is
  `1000`.
- `REAL_GAUSS_BIND_BLOCK_TARGET_INDEX`: target index used by seed-binding rebind scenarios.
  Default is `2`.

## Recommended Base Connection Settings

Use a non-pooled connection string for these scenario runs unless the scenario is explicitly
about pooling behavior:

```powershell
$env:REAL_GAUSS_EXTRA = "Database=postgres;Username=root;Password=<password>;Pooling=false;Timeout=5;Command Timeout=5;Multiplexing=false"
```

Reasons:

- Each open should establish a fresh physical route so node selection is easy to observe.
- Pool reuse can hide the actual routing result of the current open attempt.
- Lower timeout values keep failure scenarios and matrix runs shorter.

## Example: Run A Single Scenario

```powershell
Set-Location D:\netdriver\gaussdb-dotnet

$env:REAL_GAUSS_TARGETS = "10.0.0.11:8000,10.0.0.12:8000,10.0.1.11:8000"
$env:REAL_GAUSS_EXTRA = "Database=postgres;Username=root;Password=<password>;Pooling=false;Timeout=5;Command Timeout=5;Multiplexing=false"
$env:REAL_GAUSS_PRIORITY_SERVERS = "2"
$env:REAL_GAUSS_REFRESH_SECONDS = "1"

dotnet run --project .\test\GaussDB.RealClusterScenarios -- priorityservers1-failover-to-fallback-cluster
```

## Example: Run The Full Scenario Matrix

```powershell
Set-Location D:\netdriver\gaussdb-dotnet

$env:REAL_GAUSS_TARGETS = "10.0.0.11:8000,10.0.0.12:8000,10.0.1.11:8000"
$env:REAL_GAUSS_EXTRA = "Database=postgres;Username=root;Password=<password>;Pooling=false;Timeout=5;Command Timeout=5;Multiplexing=false"
$env:REAL_GAUSS_PRIORITY_SERVERS = "2"
$env:REAL_GAUSS_REFRESH_SECONDS = "1"
$env:REAL_GAUSS_FAIL_DELAY_MS = "1000"
$env:REAL_GAUSS_BIND_BLOCK_TARGET_INDEX = "2"

dotnet run --project .\test\GaussDB.RealClusterScenarios -- matrix
```

## Command-Line Override Form

Environment variables are optional. Every input can also be passed directly on the command
line:

```powershell
dotnet run --project .\test\GaussDB.RealClusterScenarios -- `
  matrix `
  --targets "10.0.0.11:8000,10.0.0.12:8000,10.0.1.11:8000" `
  --extra "Database=postgres;Username=root;Password=<password>;Pooling=false;Timeout=5;Command Timeout=5;Multiplexing=false" `
  --priority-servers "2" `
  --refresh-seconds "1" `
  --fail-delay-ms "1000" `
  --bind-block-target-index "2"
```

## What The Scenarios Cover

The scenario set is designed for real distributed routing behavior that cannot be validated
well through the normal single-node product profile alone. It includes:

- Open-stage failover and transient open retry behavior.
- Priority-cluster routing and fallback-cluster takeover.
- In-cluster balancing behavior for `LoadBalanceHosts` and `AutoBalance`.
- Coordinator discovery through `pgxc_node`.
- `UsingEip` host-column selection during discovery.
- Seed-to-node binding and dynamic endpoint rebind / recovery.
- Host offline cache and `HostRecheckSeconds` behavior.
- No-replay validation for SQL errors, active readers, explicit transactions, copy/export,
  and command timeout paths.

The authoritative list is whatever the runner prints from:

```powershell
dotnet run --project .\test\GaussDB.RealClusterScenarios -- list
```

## Observing The Results

Each scenario writes a readable console trace that typically includes:

- The effective connection string used for that scenario.
- The chosen seed targets or proxy endpoints.
- The node name returned by `SELECT get_nodename()`.
- Which endpoint the current open actually connected through.
- PASS / FAIL summary lines.

For matrix mode, the runner prints a final summary with one line per scenario.

## Choosing Between The Two Real-DB Test Paths

Use `docs/remote-gaussdb-local-testing.md` when you want:

- Full remote product-compatibility regression against a single GaussDB instance.
- Broad driver coverage outside distributed / HA routing behavior.

Use this real-cluster scenario runner when you want:

- Real multi-CN routing validation.
- Priority-cluster and fallback-cluster behavior.
- CN discovery and dynamic endpoint rebind validation.
- Targeted reproduction of distributed reconnect or failover behavior.

In practice, compatibility work is usually validated by both:

1. Run the standard remote product profile against a disposable remote database.
2. Run the real-cluster scenario runner against the distributed HA topology.

## Cleanup

These scenarios do not create a disposable database automatically. Cleanup is usually limited
to clearing the local environment variables after the run if needed:

```powershell
Remove-Item Env:\REAL_GAUSS_TARGETS -ErrorAction SilentlyContinue
Remove-Item Env:\REAL_GAUSS_EXTRA -ErrorAction SilentlyContinue
Remove-Item Env:\REAL_GAUSS_PRIORITY_SERVERS -ErrorAction SilentlyContinue
Remove-Item Env:\REAL_GAUSS_REFRESH_SECONDS -ErrorAction SilentlyContinue
Remove-Item Env:\REAL_GAUSS_FAIL_DELAY_MS -ErrorAction SilentlyContinue
Remove-Item Env:\REAL_GAUSS_BIND_BLOCK_TARGET_INDEX -ErrorAction SilentlyContinue
```

If a scenario fails partway through, no server-side database drop step is normally required.
However, because some scenarios terminate the current backend or force reconnect, avoid using
the same credentials for unrelated concurrent work during the run.
