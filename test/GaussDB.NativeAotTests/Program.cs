using System;
using GaussDB;

var connectionString = Environment.GetEnvironmentVariable("NPGSQL_TEST_DB")
                       ?? "Server=localhost;Username=gaussdb_tests;Password=gaussdb_tests;Database=gaussdb_tests;Timeout=0;Command Timeout=0";

var dataSourceBuilder = new GaussDBSlimDataSourceBuilder(connectionString);
await using var dataSource = dataSourceBuilder.Build();

await using var conn = dataSource.CreateConnection();
await conn.OpenAsync();
await using var cmd = new GaussDBCommand("SELECT 'Hello World'", conn);
await using var reader = await cmd.ExecuteReaderAsync();
if (!await reader.ReadAsync())
    throw new Exception("Got nothing from the database");

var value = reader.GetFieldValue<string>(0);
if (value != "Hello World")
    throw new Exception($"Got {value} instead of the expected 'Hello World'");
