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
