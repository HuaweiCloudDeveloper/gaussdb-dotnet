using BenchmarkDotNet.Attributes;
using HuaweiCloud.GaussDB.Internal;
using HuaweiCloud.GaussDB.Internal.Postgres;

namespace HuaweiCloud.GaussDB.Benchmarks;

[MemoryDiagnoser]
public class ResolveHandler
{
    GaussDBDataSource? _dataSource;
    PgSerializerOptions _serializerOptions = null!;

    [Params(0, 1, 2)]
    public int NumPlugins { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var dataSourceBuilder = new GaussDBDataSourceBuilder();
        if (NumPlugins > 0)
            dataSourceBuilder.UseNodaTime();
        if (NumPlugins > 1)
            dataSourceBuilder.UseNetTopologySuite();
        _dataSource = dataSourceBuilder.Build();
        _serializerOptions = _dataSource.SerializerOptions;
    }

    [GlobalCleanup]
    public void Cleanup() => _dataSource?.Dispose();

    [Benchmark]
    public PgTypeInfo? ResolveDefault()
        => _serializerOptions.GetTypeInfoInternal(null, new Oid(23)); // int4

    [Benchmark]
    public PgTypeInfo? ResolveType()
        => _serializerOptions.GetTypeInfoInternal(typeof(int), null);

    [Benchmark]
    public PgTypeInfo? ResolveBoth()
        => _serializerOptions.GetTypeInfoInternal(typeof(int), new Oid(23)); // int4
}
