using System.Threading.Tasks;
using GaussDB.Properties;
using GaussDBTypes;
using NUnit.Framework;

namespace GaussDB.Tests.Types;

public class LTreeTests(MultiplexingMode multiplexingMode) : MultiplexingTestBase(multiplexingMode)
{
    [Test]
    public Task LQuery()
        => AssertType("Top.Science.*", "Top.Science.*", "lquery", GaussDBDbType.LQuery, isDefaultForWriting: false);

    [Test]
    public Task LTree()
        => AssertType("Top.Science.Astronomy", "Top.Science.Astronomy", "ltree", GaussDBDbType.LTree, isDefaultForWriting: false);

    [Test]
    public Task LTxtQuery()
        => AssertType("Science & Astronomy", "Science & Astronomy", "ltxtquery", GaussDBDbType.LTxtQuery, isDefaultForWriting: false);

    [Test]
    public async Task LTree_not_supported_by_default_on_GaussDBSlimSourceBuilder()
    {
        var errorMessage = string.Format(
            GaussDBStrings.LTreeNotEnabled, nameof(GaussDBSlimDataSourceBuilder.EnableLTree), nameof(GaussDBSlimDataSourceBuilder));

        var dataSourceBuilder = new GaussDBSlimDataSourceBuilder(ConnectionString);
        await using var dataSource = dataSourceBuilder.Build();

        var exception =
            await AssertTypeUnsupportedRead<GaussDBRange<int>>("Top.Science.Astronomy", "ltree", dataSource);
        Assert.That(exception.InnerException!.Message, Is.EqualTo(errorMessage));
        exception = await AssertTypeUnsupportedWrite<string>("Top.Science.Astronomy", "ltree", dataSource);
        Assert.That(exception.InnerException!.Message, Is.EqualTo(errorMessage));
    }

    [Test]
    public async Task GaussDBSlimSourceBuilder_EnableLTree()
    {
        var dataSourceBuilder = new GaussDBSlimDataSourceBuilder(ConnectionString);
        dataSourceBuilder.EnableLTree();
        await using var dataSource = dataSourceBuilder.Build();

        await AssertType(dataSource, "Top.Science.Astronomy", "Top.Science.Astronomy", "ltree", GaussDBDbType.LTree, isDefaultForWriting: false, skipArrayCheck: true);
    }

    [Test]
    public async Task GaussDBSlimSourceBuilder_EnableArrays()
    {
        var dataSourceBuilder = new GaussDBSlimDataSourceBuilder(ConnectionString);
        dataSourceBuilder.EnableLTree();
        dataSourceBuilder.EnableArrays();
        await using var dataSource = dataSourceBuilder.Build();

        await AssertType(dataSource, "Top.Science.Astronomy", "Top.Science.Astronomy", "ltree", GaussDBDbType.LTree, isDefaultForWriting: false);
    }

    [OneTimeSetUp]
    public async Task SetUp()
    {
        await using var conn = await OpenConnectionAsync();
        TestUtil.MinimumPgVersion(conn, "13.0");
        await TestUtil.EnsureExtensionAsync(conn, "ltree");
    }
}
