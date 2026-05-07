# GaussDB HA Connection Parameters

This document describes the distributed HA-related connection parameters exposed by the GaussDB .NET driver.

## Parameters

### `PriorityServers`

Splits the configured seed host list into two AZ clusters:

- the first `PriorityServers` seed hosts form the preferred cluster;
- the remaining seed hosts form the fallback cluster.

Use this when you want the driver to try one AZ first, then fail over to the other AZ only if needed.

Default: `0`

### `LoadBalanceHosts`

Keeps the legacy host-shuffle behavior when `AutoBalance` is not explicitly set.

- `true`: shuffle the candidate hosts inside the selected cluster.
- `false`: keep the configured host order.

When `AutoBalance` is set, `LoadBalanceHosts` is ignored.

### `AutoBalance`

Controls how coordinator hosts are ordered inside the currently selected cluster.

Supported values:

- `false`
- `shuffle`
- `roundrobin`
- `true`
- `balance`
- `priorityN`
- `shuffleN`
- `specified`
- `leastconn`

Notes:

- `true` and `balance` are aliases for `roundrobin`.
- `priorityN` and `shuffleN` use the first `N` seeds as a preferred subset.
- `shufflePriorityN` is also accepted for backward compatibility and normalized to `shuffleN`.
- `AutoBalance` only changes ordering inside the chosen cluster; it does not change cluster selection itself.

### `HostRecheckSeconds`

Controls how long a failed host/CN stays offline before it can be probed again.

- `0` means immediate recheck.
- A positive value keeps the host in an offline cache for that many seconds.

Default: `10`

### `RefreshCNIpListTime`

Controls how often the driver refreshes CN metadata.

- `0` disables refresh and uses only the configured seed hosts.
- A positive value enables periodic discovery from metadata.

Refresh failures are throttled instead of being retried continuously.

Default: `0`

### `UsingEip`

Selects which metadata columns are used during CN refresh:

- `true`: use `node_host1` / `node_port1`
- `false`: use `node_host` / `node_port`

Default: `true`

### `DisasterToleranceCluster`

When enabled on a disaster-tolerance deployment, coordinator refresh uses `pgxc_disaster_read_node()` instead of `pgxc_node`, matching JDBC behavior.

Default: `false`

### `AutoReconnect`

Enables bounded reconnect during `Open` / `OpenAsync` for eligible connection and failover errors.

Important:

- it only applies while opening the connection;
- it does not replay commands after the connection has already been opened;
- it does not retry arbitrary SQL errors.

Default: `false`

### `MaxReconnects`

Controls how many reconnect attempts are allowed when `AutoReconnect` is enabled.

Default: `1`

## Example

```csharp
var csb = new GaussDBConnectionStringBuilder
{
    Host = "cn1:8000,cn2:8000,cn3:8000,cn4:8000",
    PriorityServers = 2,
    AutoBalance = "priority2",
    LoadBalanceHosts = true,
    RefreshCNIpListTime = 10,
    UsingEip = true,
    DisasterToleranceCluster = true,
    AutoReconnect = true,
    MaxReconnects = 3,
    HostRecheckSeconds = 10
};

await using var dataSource = new GaussDBDataSourceBuilder(csb.ConnectionString)
    .BuildMultiHost();
await using var conn = await dataSource
    .WithTargetSession(TargetSessionAttributes.Primary)
    .OpenConnectionAsync();
```

## Practical Guidance

- Use `PriorityServers` to define the preferred AZ split.
- Use `AutoBalance` to control host ordering inside the selected cluster.
- Use `LoadBalanceHosts` only when you want the legacy shuffle behavior and have not set `AutoBalance`.
- Use `RefreshCNIpListTime` and `UsingEip` only when metadata-based CN discovery is needed.
- Use `AutoReconnect` and `MaxReconnects` for bounded open-time reconnects.
- Use `HostRecheckSeconds` to control how quickly failed hosts are reconsidered.
