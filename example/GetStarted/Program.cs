using System.Data;
using HuaweiCloud.GaussDB;

dotenv.net.DotEnv.Load();

var connString = Environment.GetEnvironmentVariable("GaussdbConnString");
ArgumentNullException.ThrowIfNull(connString);
// ReSharper disable AccessToDisposedClosure
// Dispose will be handled
await using var conn = new GaussDBConnection(connString);
if (conn.State is ConnectionState.Closed)
{
    await conn.OpenAsync();
}

Console.WriteLine($@"Connection state: {conn.State}");

await ConnectionTest();

var tableName = "employees";
// id int, name varchar(128), age int
{
    await ExecuteTableScript(async () =>
    {
        // insert
        {
            var insertSql = $"""
                             INSERT INTO {tableName} (id, name, age) VALUES
                             (1, 'John', 30),
                             (2, 'Alice', 16),
                             (3, 'Mike', 24)
                             """;
            await using var cmd = new GaussDBCommand(insertSql, conn);
            await cmd.ExecuteNonQueryAsync();
            Console.WriteLine(@"insert script executed");
        }

        // query
        {
            await QueryTest();
        }

        // update
        {
            var updateSql = $"""
                             UPDATE {tableName}
                             SET age = 18
                             WHERE name = @name
                             """;
            await using var cmd = new GaussDBCommand(updateSql, conn);
            cmd.Parameters.AddWithValue("name", "Alice");
            await cmd.ExecuteNonQueryAsync();
            Console.WriteLine(@"update script executed");
        }

        // query
        {
            await QueryTest("age >= 18");
        }

        // delete
        {
            var deleteSql = $"""
                             DELETE FROM {tableName}
                             WHERE age > @age
                             """;
            await using var cmd = new GaussDBCommand(deleteSql, conn);
            cmd.Parameters.AddWithValue("age", 10);
            await cmd.ExecuteNonQueryAsync();
            Console.WriteLine(@"delete script executed");
        }

        // query count
        {
            var queryCountSql = $"SELECT COUNT(*) FROM {tableName}";
            await using var cmd = new GaussDBCommand(queryCountSql, conn);
            var result = await cmd.ExecuteScalarAsync();
            var count = Convert.ToInt32(result);
            Console.WriteLine($@"count: {count}");
        }
    });
}

Console.WriteLine(@"Completed!");


async Task ConnectionTest()
{
    await using var cmd = new GaussDBCommand("SELECT 1", conn);
    var result = await cmd.ExecuteScalarAsync();
    Console.WriteLine(result);
}

async Task ExecuteTableScript(Func<Task> func)
{
    var createTableSql = $"""
               DROP TABLE IF EXISTS {tableName} CASCADE;
               CREATE TABLE {tableName}
               (
                   id INT PRIMARY KEY,
                   name VARCHAR(128),
                   age INT
               );
               """;
    var dropTableSql = $"DROP TABLE IF EXISTS {tableName} CASCADE;";

    try
    {
        await using var createTableCommand = new GaussDBCommand(createTableSql, conn);
        await createTableCommand.ExecuteNonQueryAsync();

        await func();
    }
    catch (Exception ex)
    {
        Console.WriteLine(ex);
    }
    finally
    {
        try
        {
            await using var dropTableCommand = new GaussDBCommand(dropTableSql, conn);
            await dropTableCommand.ExecuteNonQueryAsync();
        }
        catch (Exception e)
        {
            Console.WriteLine($@"Exception when drop table {tableName} {e}");
        }
    }

}

async Task QueryTest(string? condition = null)
{
    var sql = $"SELECT * FROM {tableName}";
    if (!string.IsNullOrEmpty(condition))
    {
        sql += $" WHERE {condition}";
    }
    Console.WriteLine($@"Executing query {sql}");
    await using (var cmd = new GaussDBCommand(sql, conn))
    {
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var id = reader.GetInt32("id");
            var name = reader.GetString("name");
            var age = reader.GetInt32("age");

            Console.WriteLine($@"ID: {id}, Name: {name}, Age: {age}");
        }
    }
}
