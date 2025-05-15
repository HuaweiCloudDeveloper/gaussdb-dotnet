using System.Threading.Tasks;
using HuaweiCloud.GaussDBTypes;
using NUnit.Framework;

namespace HuaweiCloud.GaussDB.Tests.Types;

/// <summary>
/// Tests on PostgreSQL geometric types
/// </summary>
/// <remarks>
/// https://www.postgresql.org/docs/current/static/datatype-geometric.html
/// </remarks>
class GeometricTypeTests(MultiplexingMode multiplexingMode) : MultiplexingTestBase(multiplexingMode)
{
    //todo: 不支持point,运行时报错
    /*[Test]
    public Task Point()
        => AssertType(new GaussDBPoint(1.2, 3.4), "(1.2,3.4)", "point", GaussDBDbType.Point);*/

    //todo: 不支持line
    /*[Test]
    public Task Line()
        => AssertType(new GaussDBLine(1, 2, 3), "{1,2,3}", "line", GaussDBDbType.Line);*/

    [Test]
    public Task LineSegment()
        => AssertType(new GaussDBLSeg(1, 2, 3, 4), "[(1,2),(3,4)]", "lseg", GaussDBDbType.LSeg);

    [Test]
    public async Task Box()
    {
        await AssertType(
            new GaussDBBox(top: 3, right: 4, bottom: 1, left: 2),
            "(4,3),(2,1)",
            "box",
            GaussDBDbType.Box,
            skipArrayCheck: true); // Uses semicolon instead of comma as separator

        await AssertType(
            new GaussDBBox(top: -10, right: 0, bottom: -20, left: -10),
            "(0,-10),(-10,-20)",
            "box",
            GaussDBDbType.Box,
            skipArrayCheck: true); // Uses semicolon instead of comma as separator

        await AssertType(
            new GaussDBBox(top: 1, right: 2, bottom: 3, left: 4),
            "(4,3),(2,1)",
            "box",
            GaussDBDbType.Box,
            skipArrayCheck: true); // Uses semicolon instead of comma as separator

        var swapped = new GaussDBBox(top: -20, right: -10, bottom: -10, left: 0);

        await AssertType(
            swapped,
            "(0,-10),(-10,-20)",
            "box",
            GaussDBDbType.Box,
            skipArrayCheck: true); // Uses semicolon instead of comma as separator

        await AssertType(
            swapped with { UpperRight = new GaussDBPoint(-20,-10) },
            "(-10,-10),(-20,-20)",
            "box",
            GaussDBDbType.Box,
            skipArrayCheck: true); // Uses semicolon instead of comma as separator

        await AssertType(
            swapped with { LowerLeft = new GaussDBPoint(10, 10) },
            "(10,10),(0,-10)",
            "box",
            GaussDBDbType.Box,
            skipArrayCheck: true); // Uses semicolon instead of comma as separator
    }

    [Test]
    public async Task Box_array()
    {
        var data = new[]
        {
            new GaussDBBox(top: 3, right: 4, bottom: 1, left: 2),
            new GaussDBBox(top: 5, right: 6, bottom: 3, left: 4),
            new GaussDBBox(top: -10, right: 0, bottom: -20, left: -10)
        };

        await AssertType(
            data,
            "{(4,3),(2,1);(6,5),(4,3);(0,-10),(-10,-20)}",
            "box[]",
            GaussDBDbType.Box | GaussDBDbType.Array
            );

        var swappedData = new[]
        {
            new GaussDBBox(top: 1, right: 2, bottom: 3, left: 4),
            new GaussDBBox(top: 3, right: 4, bottom: 5, left: 6),
            new GaussDBBox(top: -20, right: -10, bottom: -10, left: 0)
        };

        await AssertType(
            swappedData,
            "{(4,3),(2,1);(6,5),(4,3);(0,-10),(-10,-20)}",
            "box[]",
            GaussDBDbType.Box | GaussDBDbType.Array
            );
    }

    [Test]
    public Task Path_closed()
        => AssertType(
            new GaussDBPath([new GaussDBPoint(1, 2), new GaussDBPoint(3, 4)], false),
            "((1,2),(3,4))",
            "path",
            GaussDBDbType.Path);

    [Test]
    public Task Path_open()
        => AssertType(
            new GaussDBPath([new GaussDBPoint(1, 2), new GaussDBPoint(3, 4)], true),
            "[(1,2),(3,4)]",
            "path",
            GaussDBDbType.Path);

    [Test]
    public Task Polygon()
        => AssertType(
            new GaussDBPolygon(new GaussDBPoint(1, 2), new GaussDBPoint(3, 4)),
            "((1,2),(3,4))",
            "polygon",
            GaussDBDbType.Polygon);

    [Test]
    public Task Circle()
        => AssertType(
            new GaussDBCircle(1, 2, 0.5),
            "<(1,2),0.5>",
            "circle",
            GaussDBDbType.Circle);
}
