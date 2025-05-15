using System;
using System.Data.Common;
using AdoNet.Specification.Tests;

namespace HuaweiCloud.GaussDB.Specification.Tests;

public class GaussDBDbFactoryFixture : IDbFactoryFixture
{
    public DbProviderFactory Factory => GaussDBFactory.Instance;

    const string DefaultConnectionString =
        "Server=localhost;Username=gaussdb_tests;Password=gaussdb_tests;Database=gaussdb_tests;Timeout=0;Command Timeout=0";

    public string ConnectionString =>
        Environment.GetEnvironmentVariable("NPGSQL_TEST_DB") ?? DefaultConnectionString;
}
