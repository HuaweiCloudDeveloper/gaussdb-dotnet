using System;
using System.Collections;
using System.Threading.Tasks;
using GaussDB.Properties;
using GaussDBTypes;
using NUnit.Framework;

#pragma warning disable CS0618 // GaussDBTsVector.Parse is obsolete

namespace GaussDB.Tests.Types;

public class FullTextSearchTests(MultiplexingMode multiplexingMode) : MultiplexingTestBase(multiplexingMode)
{
    [Test]
    public Task TsVector()
        => AssertType(
            GaussDBTsVector.Parse("'1' '2' 'a':24,25A,26B,27,28,12345C 'b' 'c' 'd'"),
            "'1' '2' 'a':24,25A,26B,27,28,12345C 'b' 'c' 'd'",
            "tsvector",
            GaussDBDbType.TsVector);

    public static IEnumerable TsQueryTestCases() => new[]
    {
        [
            "'a'",
            new GaussDBTsQueryLexeme("a")
        ],
        [
            "!'a'",
            new GaussDBTsQueryNot(
                new GaussDBTsQueryLexeme("a"))
        ],
        [
            "'a' | 'b'",
            new GaussDBTsQueryOr(
                new GaussDBTsQueryLexeme("a"),
                new GaussDBTsQueryLexeme("b"))
        ],
        [
            "'a' & 'b'",
            new GaussDBTsQueryAnd(
                new GaussDBTsQueryLexeme("a"),
                new GaussDBTsQueryLexeme("b"))
        ],
        new object[]
        {
            "'a' <-> 'b'",
            new GaussDBTsQueryFollowedBy(
                new GaussDBTsQueryLexeme("a"), 1, new GaussDBTsQueryLexeme("b"))
        }
    };

    //todo: 无法识别无效的操作符tsquery
    /*[Test]
    [TestCaseSource(nameof(TsQueryTestCases))]
    public Task TsQuery(string sqlLiteral, GaussDBTsQuery query)
        => AssertType(query, sqlLiteral, "tsquery", GaussDBDbType.TsQuery);*/

    [Test]
    public async Task Full_text_search_not_supported_by_default_on_GaussDBSlimSourceBuilder()
    {
        var errorMessage = string.Format(
            GaussDBStrings.FullTextSearchNotEnabled,
            nameof(GaussDBSlimDataSourceBuilder.EnableFullTextSearch),
            nameof(GaussDBSlimDataSourceBuilder));

        var dataSourceBuilder = new GaussDBSlimDataSourceBuilder(ConnectionString);
        await using var dataSource = dataSourceBuilder.Build();

        var exception = await AssertTypeUnsupportedRead<GaussDBTsQuery, InvalidCastException>("a", "tsquery", dataSource);
        Assert.IsInstanceOf<NotSupportedException>(exception.InnerException);
        Assert.AreEqual(errorMessage, exception.InnerException!.Message);

        exception = await AssertTypeUnsupportedWrite<GaussDBTsQuery, InvalidCastException>(new GaussDBTsQueryLexeme("a"), pgTypeName: null, dataSource);
        Assert.IsInstanceOf<NotSupportedException>(exception.InnerException);
        Assert.AreEqual(errorMessage, exception.InnerException!.Message);

        exception = await AssertTypeUnsupportedRead<GaussDBTsVector, InvalidCastException>("1", "tsvector", dataSource);
        Assert.IsInstanceOf<NotSupportedException>(exception.InnerException);
        Assert.AreEqual(errorMessage, exception.InnerException!.Message);

        exception = await AssertTypeUnsupportedWrite<GaussDBTsVector, InvalidCastException>(GaussDBTsVector.Parse("'1'"), pgTypeName: null, dataSource);
        Assert.IsInstanceOf<NotSupportedException>(exception.InnerException);
        Assert.AreEqual(errorMessage, exception.InnerException!.Message);
    }

    [Test]
    public async Task GaussDBSlimSourceBuilder_EnableFullTextSearch()
    {
        var dataSourceBuilder = new GaussDBSlimDataSourceBuilder(ConnectionString);
        dataSourceBuilder.EnableFullTextSearch();
        await using var dataSource = dataSourceBuilder.Build();

        await AssertType<GaussDBTsQuery>(new GaussDBTsQueryLexeme("a"), "'a'", "tsquery", GaussDBDbType.TsQuery);
        await AssertType(GaussDBTsVector.Parse("'1'"), "'1'", "tsvector", GaussDBDbType.TsVector);
    }
}
