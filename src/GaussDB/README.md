GaussDB is the open source .NET data provider for GaussDB/OpenGauss. It allows you to connect and interact with GaussDB/OpenGauss server using .NET.

## Quickstart

Here's a basic code snippet to get you started:

```csharp
var connString = "Host=myserver;Username=mylogin;Password=mypass;Database=mydatabase";

await using var conn = new GaussDBConnection(connString);
await conn.OpenAsync();

// Insert some data
await using (var cmd = new GaussDBCommand("INSERT INTO data (some_field) VALUES (@p)", conn))
{
    cmd.Parameters.AddWithValue("p", "Hello world");
    await cmd.ExecuteNonQueryAsync();
}

// Retrieve all rows
await using (var cmd = new GaussDBCommand("SELECT some_field FROM data", conn))
await using (var reader = await cmd.ExecuteReaderAsync())
{
  while (await reader.ReadAsync())
    Console.WriteLine(reader.GetString(0));
}
```

## Distributed HA options

The driver also supports JDBC-aligned distributed GaussDB HA routing options on top of the existing multi-host support:

- `PriorityServers`: splits the seed host list into preferred and fallback AZ clusters.
- `AutoBalance`: reorders coordinator nodes only inside the selected cluster. Supported values include `shuffle`, `roundrobin`, `priorityN`, and `shufflePriorityN`.
- `RefreshCNIpListTime`: throttles coordinator discovery from `pgxc_node`, including repeated refresh failures.
- `UsingEip`: selects `node_host1/node_port1` instead of `node_host/node_port` during coordinator refresh.
- `AutoReconnect`: enables bounded reconnect for eligible disconnect and failover errors during `Open/OpenAsync()` and safe command execution windows.
- `MaxReconnects`: caps the reconnect attempt count.
- `HostRecheckSeconds`: controls how long failed hosts/CNs stay offline before they can be probed again.

Example:

```csharp
var csb = new GaussDBConnectionStringBuilder
{
    Host = "cn1:8000,cn2:8000,cn3:8000,cn4:8000",
    PriorityServers = 2,
    AutoBalance = "priority2",
    RefreshCNIpListTime = 10,
    UsingEip = true,
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

`PriorityServers` and `AutoBalance` work at different layers: `PriorityServers` picks which AZ cluster is tried first, while `AutoBalance` only changes coordinator ordering inside that selected cluster.

`TargetSessionAttributes` still applies on top of HA routing: the driver first builds the HA candidate list and then filters candidates by the requested server type (`primary`, `standby`, `read-write`, `read-only`, and the `prefer-*` variants).

`HostRecheckSeconds` continues to drive abnormal CN eviction and recheck timing. Failed CNs are marked offline and skipped until the recheck window expires; after that they return to `Unknown` and can be probed again by later opens or reconnects.

Automatic reconnect is intentionally conservative. It retries eligible disconnect and failover errors by reopening through the latest routing state during initial open and safe command execution windows, but it does not transparently replay explicit transactions, COPY operations, or active streaming readers. This keeps the behavior close to JDBC's HA intent without introducing unsafe generic statement replay in .NET.
