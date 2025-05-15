using System;

namespace GaussDB.Benchmarks;

static class BenchmarkEnvironment
{
    internal static string ConnectionString => Environment.GetEnvironmentVariable("NPGSQL_TEST_DB") ?? DefaultConnectionString;

    /// <summary>
    /// Unless the NPGSQL_TEST_DB environment variable is defined, this is used as the connection string for the
    /// test database.
    /// </summary>
    const string DefaultConnectionString = "Server=localhost;User ID=gaussdb_tests;Password=gaussdb_tests;Database=gaussdb_tests";

    internal static GaussDBConnection GetConnection() => new(ConnectionString);

    internal static GaussDBConnection OpenConnection()
    {
        var conn = GetConnection();
        conn.Open();
        return conn;
    } 
}