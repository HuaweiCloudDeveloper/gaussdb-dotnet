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
    await using (var cmd = new NpgsqlCommand("SELECT * FROM employees", conn))
    {
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            // 读取每一行数据
            var id = reader.GetInt32(0); // 假设第一列是ID，类型为int
            var name = reader.GetString(1); // 假设第二列是Name，类型为varchar
            var age = reader.GetInt32(2); // 假设第三列是Age，可为空

            Console.WriteLine($"ID: {id}, Name: {name}, Age: {age}");
        }
    }
}

Console.WriteLine(@"Completed!");
