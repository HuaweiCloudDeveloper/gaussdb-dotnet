using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using Microsoft.Data.SqlClient;

// ReSharper disable UnusedMember.Global

namespace HuaweiCloud.GaussDB.Benchmarks;

[Config(typeof(Config))]
public class ConnectionCreationBenchmarks
{
    const string GaussDBConnectionString = "Host=foo;Database=bar;Username=user;Password=password";
    const string SqlClientConnectionString = @"Data Source=(localdb)\mssqllocaldb";

    [Benchmark]
    public GaussDBConnection GaussDB() => new(GaussDBConnectionString);

    [Benchmark]
    public SqlConnection SqlClient() => new(SqlClientConnectionString);

    class Config : ManualConfig
    {
        public Config()
            => AddColumn(StatisticColumn.OperationsPerSecond);
    }
}
