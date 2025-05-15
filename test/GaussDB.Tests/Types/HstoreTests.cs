using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading.Tasks;
using HuaweiCloud.GaussDBTypes;
using NUnit.Framework;

namespace HuaweiCloud.GaussDB.Tests.Types;

//todo: 该扩展功能不安全，可能会引发意外情况。
/*public class HstoreTests(MultiplexingMode multiplexingMode) : MultiplexingTestBase(multiplexingMode)
{
    [Test]
    public Task Hstore()
        => AssertType(
            new Dictionary<string, string?>
            {
                {"a", "3"},
                {"b", null},
                {"cd", "hello"}
            },
            @"""a""=>""3"", ""b""=>NULL, ""cd""=>""hello""",
            "hstore",
            GaussDBDbType.Hstore, isGaussDBDbTypeInferredFromClrType: false);

    [Test]
    public Task Hstore_empty()
        => AssertType(new Dictionary<string, string?>(), @"", "hstore", GaussDBDbType.Hstore, isGaussDBDbTypeInferredFromClrType: false);

    [Test]
    public Task Hstore_as_ImmutableDictionary()
    {
        var builder = ImmutableDictionary<string, string?>.Empty.ToBuilder();
        builder.Add("a", "3");
        builder.Add("b", null);
        builder.Add("cd", "hello");
        var immutableDictionary = builder.ToImmutableDictionary();

        return AssertType(
            immutableDictionary,
            @"""a""=>""3"", ""b""=>NULL, ""cd""=>""hello""",
            "hstore",
            GaussDBDbType.Hstore,
            isDefaultForReading: false, isGaussDBDbTypeInferredFromClrType: false);
    }

    [Test]
    public Task Hstore_as_IDictionary()
        => AssertType<IDictionary<string, string?>>(
            new Dictionary<string, string?>
            {
                { "a", "3" },
                { "b", null },
                { "cd", "hello" }
            },
            @"""a""=>""3"", ""b""=>NULL, ""cd""=>""hello""",
            "hstore",
            GaussDBDbType.Hstore,
            isDefaultForReading: false, isGaussDBDbTypeInferredFromClrType: false);

    [OneTimeSetUp]
    public async Task SetUp()
    {
        using var conn = await OpenConnectionAsync();
        TestUtil.MinimumPgVersion(conn, "9.1", "Hstore introduced in PostgreSQL 9.1");
        await TestUtil.EnsureExtensionAsync(conn, "hstore", "9.1");
    }
}*/
