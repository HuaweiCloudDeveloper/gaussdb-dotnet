using System.Data;
using Npgsql;

dotenv.net.DotEnv.Load();

var connString = Environment.GetEnvironmentVariable("GaussdbConnString");
ArgumentNullException.ThrowIfNull(connString);

await using var conn = new NpgsqlConnection(connString);
if (conn.State is ConnectionState.Closed)
{
    await conn.OpenAsync();
}

Console.WriteLine($@"Connection state: {conn.State}");

{
    await using var cmd = new NpgsqlCommand("SELECT 1", conn);
    var result = await cmd.ExecuteScalarAsync();
    Console.WriteLine(result);
}

Console.WriteLine(@"Completed!");
