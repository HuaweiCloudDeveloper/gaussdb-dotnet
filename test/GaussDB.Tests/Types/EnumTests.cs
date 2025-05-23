using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using HuaweiCloud.GaussDB.NameTranslation;
using HuaweiCloud.GaussDB.PostgresTypes;
using HuaweiCloud.GaussDB.Properties;
using HuaweiCloud.GaussDBTypes;
using NUnit.Framework;
using static HuaweiCloud.GaussDB.Tests.TestUtil;

namespace HuaweiCloud.GaussDB.Tests.Types;

public class EnumTests(MultiplexingMode multiplexingMode) : MultiplexingTestBase(multiplexingMode)
{
    enum Mood { Sad, Ok, Happy }
    enum AnotherEnum { Value1, Value2 }

    [Test]
    public async Task Data_source_mapping()
    {
        await using var adminConnection = await OpenConnectionAsync();
        var type = await GetTempTypeName(adminConnection);
        await adminConnection.ExecuteNonQueryAsync($"CREATE TYPE {type} AS ENUM ('sad', 'ok', 'happy')");

        var dataSourceBuilder = CreateDataSourceBuilder();
        dataSourceBuilder.MapEnum<Mood>(type);
        await using var dataSource = dataSourceBuilder.Build();

        await AssertType(dataSource, Mood.Happy, "happy", type, gaussdbDbType: null);
    }

    [Test]
    public async Task Data_source_unmap()
    {
        await using var adminConnection = await OpenConnectionAsync();
        var type = await GetTempTypeName(adminConnection);
        await adminConnection.ExecuteNonQueryAsync($"CREATE TYPE {type} AS ENUM ('sad', 'ok', 'happy')");

        var dataSourceBuilder = CreateDataSourceBuilder();
        dataSourceBuilder.MapEnum<Mood>(type);

        var isUnmapSuccessful = dataSourceBuilder.UnmapEnum<Mood>(type);
        await using var dataSource = dataSourceBuilder.Build();

        Assert.IsTrue(isUnmapSuccessful);
        Assert.ThrowsAsync<InvalidCastException>(() => AssertType(dataSource, Mood.Happy, "happy", type, gaussdbDbType: null));
    }

    [Test]
    public async Task Data_source_mapping_non_generic()
    {
        await using var adminConnection = await OpenConnectionAsync();
        var type = await GetTempTypeName(adminConnection);
        await adminConnection.ExecuteNonQueryAsync($"CREATE TYPE {type} AS ENUM ('sad', 'ok', 'happy')");

        var dataSourceBuilder = CreateDataSourceBuilder();
        dataSourceBuilder.MapEnum(typeof(Mood), type);
        await using var dataSource = dataSourceBuilder.Build();
        await AssertType(dataSource, Mood.Happy, "happy", type, gaussdbDbType: null);
    }

    [Test]
    public async Task Data_source_unmap_non_generic()
    {
        await using var adminConnection = await OpenConnectionAsync();
        var type = await GetTempTypeName(adminConnection);
        await adminConnection.ExecuteNonQueryAsync($"CREATE TYPE {type} AS ENUM ('sad', 'ok', 'happy')");

        var dataSourceBuilder = CreateDataSourceBuilder();
        dataSourceBuilder.MapEnum(typeof(Mood), type);

        var isUnmapSuccessful = dataSourceBuilder.UnmapEnum(typeof(Mood), type);
        await using var dataSource = dataSourceBuilder.Build();

        Assert.IsTrue(isUnmapSuccessful);
        Assert.ThrowsAsync<InvalidCastException>(() => AssertType(dataSource, Mood.Happy, "happy", type, gaussdbDbType: null));
    }

    [Test]
    public async Task Dual_enums()
    {
        await using var adminConnection = await OpenConnectionAsync();
        var type1 = await GetTempTypeName(adminConnection);
        var type2 = await GetTempTypeName(adminConnection);
        await adminConnection.ExecuteNonQueryAsync($@"
CREATE TYPE {type1} AS ENUM ('sad', 'ok', 'happy');
CREATE TYPE {type2} AS ENUM ('label1', 'label2', 'label3')");

        var dataSourceBuilder = CreateDataSourceBuilder();
        dataSourceBuilder.MapEnum<Mood>(type1);
        dataSourceBuilder.MapEnum<TestEnum>(type2);
        await using var dataSource = dataSourceBuilder.Build();

        await AssertType(dataSource, new[] { Mood.Ok, Mood.Sad }, "{ok,sad}", type1 + "[]", gaussdbDbType: null);
    }

    [Test]
    public async Task Array()
    {
        await using var adminConnection = await OpenConnectionAsync();
        var type = await GetTempTypeName(adminConnection);
        await adminConnection.ExecuteNonQueryAsync($"CREATE TYPE {type} AS ENUM ('sad', 'ok', 'happy')");

        var dataSourceBuilder = CreateDataSourceBuilder();
        dataSourceBuilder.MapEnum<Mood>(type);
        await using var dataSource = dataSourceBuilder.Build();

        await AssertType(dataSource, new[] { Mood.Ok, Mood.Happy }, "{ok,happy}", type + "[]", gaussdbDbType: null);
    }

    [Test, IssueLink("https://github.com/npgsql/npgsql/issues/859")]
    public async Task Name_translation_default_snake_case()
    {
        await using var adminConnection = await OpenConnectionAsync();
        var enumName1 = await GetTempTypeName(adminConnection);
        await adminConnection.ExecuteNonQueryAsync($"CREATE TYPE {enumName1} AS ENUM ('simple', 'two_words', 'some_database_name')");

        var dataSourceBuilder = CreateDataSourceBuilder();
        dataSourceBuilder.MapEnum<NameTranslationEnum>(enumName1);
        await using var dataSource = dataSourceBuilder.Build();

        await AssertType(dataSource, NameTranslationEnum.Simple, "simple", enumName1, gaussdbDbType: null);
        await AssertType(dataSource, NameTranslationEnum.TwoWords, "two_words", enumName1, gaussdbDbType: null);
        await AssertType(dataSource, NameTranslationEnum.SomeClrName, "some_database_name", enumName1, gaussdbDbType: null);
    }

    [Test, IssueLink("https://github.com/npgsql/npgsql/issues/859")]
    public async Task Name_translation_null()
    {
        await using var adminConnection = await OpenConnectionAsync();
        var type = await GetTempTypeName(adminConnection);
        await adminConnection.ExecuteNonQueryAsync($"CREATE TYPE {type} AS ENUM ('Simple', 'TwoWords', 'some_database_name')");

        var dataSourceBuilder = CreateDataSourceBuilder();
        dataSourceBuilder.MapEnum<NameTranslationEnum>(type, nameTranslator: new GaussDBNullNameTranslator());
        await using var dataSource = dataSourceBuilder.Build();

        await AssertType(dataSource, NameTranslationEnum.Simple, "Simple", type, gaussdbDbType: null);
        await AssertType(dataSource, NameTranslationEnum.TwoWords, "TwoWords", type, gaussdbDbType: null);
        await AssertType(dataSource, NameTranslationEnum.SomeClrName, "some_database_name", type, gaussdbDbType: null);
    }

    [Test]
    public async Task Unmapped_enum_as_clr_enum()
    {
        await using var dataSource = CreateDataSource(b => b.EnableUnmappedTypes());
        await using var connection = await dataSource.OpenConnectionAsync();
        var type1 = await GetTempTypeName(connection);
        var type2 = await GetTempTypeName(connection);
        await connection.ExecuteNonQueryAsync(@$"
CREATE TYPE {type1} AS ENUM ('sad', 'ok', 'happy');
CREATE TYPE {type2} AS ENUM ('value1', 'value2');");
        await connection.ReloadTypesAsync();

        await AssertType(connection, Mood.Happy, "happy", type1, gaussdbDbType: null, isDefault: false);
        await AssertType(connection, AnotherEnum.Value2, "value2", type2, gaussdbDbType: null, isDefault: false);
    }

    [Test]
    public async Task Unmapped_enum_as_clr_enum_supported_only_with_EnableUnmappedTypes()
    {
        await using var connection = await DataSource.OpenConnectionAsync();
        var enumType = await GetTempTypeName(connection);
        await connection.ExecuteNonQueryAsync($"CREATE TYPE {enumType} AS ENUM ('sad', 'ok', 'happy')");
        await connection.ReloadTypesAsync();

        var errorMessage = string.Format(
            GaussDBStrings.UnmappedEnumsNotEnabled,
            nameof(GaussDBSlimDataSourceBuilder.EnableUnmappedTypes),
            nameof(GaussDBDataSourceBuilder));

        var exception = await AssertTypeUnsupportedWrite(Mood.Happy, enumType);
        Assert.IsInstanceOf<NotSupportedException>(exception.InnerException);
        Assert.That(exception.InnerException!.Message, Is.EqualTo(errorMessage));

        exception = await AssertTypeUnsupportedRead<Mood>("happy", enumType);
        Assert.IsInstanceOf<NotSupportedException>(exception.InnerException);
        Assert.That(exception.InnerException!.Message, Is.EqualTo(errorMessage));
    }

    [Test]
    public async Task Unmapped_enum_as_string()
    {
        await using var connection = await OpenConnectionAsync();
        var type = await GetTempTypeName(connection);
        await connection.ExecuteNonQueryAsync($"CREATE TYPE {type} AS ENUM ('sad', 'ok', 'happy')");
        await connection.ReloadTypesAsync();

        await AssertType(connection, "happy", "happy", type, gaussdbDbType: null, isDefaultForWriting: false);
    }

    enum NameTranslationEnum
    {
        Simple,
        TwoWords,
        [PgName("some_database_name")]
        SomeClrName
    }

    [Test, IssueLink("https://github.com/npgsql/npgsql/issues/632")]
    public async Task Same_name_in_different_schemas()
    {
        await using var adminConnection = await OpenConnectionAsync();
        var schema1 = await CreateTempSchema(adminConnection);
        var schema2 = await CreateTempSchema(adminConnection);
        await adminConnection.ExecuteNonQueryAsync($@"
CREATE TYPE {schema1}.my_enum AS ENUM ('one');
CREATE TYPE {schema2}.my_enum AS ENUM ('alpha');");

        var dataSourceBuilder = CreateDataSourceBuilder();
        dataSourceBuilder.MapEnum<Enum1>($"{schema1}.my_enum");
        dataSourceBuilder.MapEnum<Enum2>($"{schema2}.my_enum");
        await using var dataSource = dataSourceBuilder.Build();

        await AssertType(dataSource, Enum1.One, "one", $"{schema1}.my_enum", gaussdbDbType: null);
        await AssertType(dataSource, Enum2.Alpha, "alpha", $"{schema2}.my_enum", gaussdbDbType: null);
    }

    enum Enum1 { One }
    enum Enum2 { Alpha }

    [Test, IssueLink("https://github.com/npgsql/npgsql/issues/1779")]
    public async Task GetPostgresType()
    {
        await using var dataSource = CreateDataSource();
        using var conn = await dataSource.OpenConnectionAsync();
        var type = await GetTempTypeName(conn);
        await conn.ExecuteNonQueryAsync($"CREATE TYPE {type} AS ENUM ('sad', 'ok', 'happy')");
        conn.ReloadTypes();

        using var cmd = new GaussDBCommand($"SELECT 'ok'::{type}", conn);
        using var reader = await cmd.ExecuteReaderAsync();
        reader.Read();
        var enumType = (PostgresEnumType)reader.GetPostgresType(0);
        Assert.That(enumType.Name, Is.EqualTo(type));
        Assert.That(enumType.Labels, Is.EqualTo(new List<string> { "sad", "ok", "happy" }));
    }

    enum TestEnum
    {
        label1,
        label2,
        [PgName("label3")]
        Label3
    }
}
