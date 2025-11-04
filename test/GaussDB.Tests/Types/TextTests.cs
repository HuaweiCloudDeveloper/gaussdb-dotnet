using System;
using System.Data;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using HuaweiCloud.GaussDBTypes;
using NUnit.Framework;
using static HuaweiCloud.GaussDB.Tests.TestUtil;

namespace HuaweiCloud.GaussDB.Tests.Types;

/// <summary>
/// Tests on PostgreSQL text
/// </summary>
/// <remarks>
/// https://www.postgresql.org/docs/current/static/datatype-character.html
/// </remarks>
public class TextTests(MultiplexingMode multiplexingMode) : MultiplexingTestBase(multiplexingMode)
{
    [Test]
    public Task Text_as_string()
        => AssertType("foo", "foo", "text", GaussDBDbType.Text, DbType.String);

    [Test]
    public Task Text_as_array_of_chars()
        => AssertType("foo".ToCharArray(), "foo", "text", GaussDBDbType.Text, DbType.String, isDefaultForReading: false);

    [Test]
    public Task Text_as_ArraySegment_of_chars()
        => AssertTypeWrite(new ArraySegment<char>("foo".ToCharArray()), "foo", "text", GaussDBDbType.Text, DbType.String,
            isDefault: false);

    [Test]
    public Task Text_as_array_of_bytes()
        => AssertType(Encoding.UTF8.GetBytes("foo"), "foo", "text", GaussDBDbType.Text, DbType.String, isDefault: false);

    [Test]
    public Task Text_as_ReadOnlyMemory_of_bytes()
        => AssertTypeWrite(new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes("foo")), "foo", "text", GaussDBDbType.Text, DbType.String,
            isDefault: false);

    [Test]
    public Task Char_as_char()
        => AssertType('f', "f", "character", GaussDBDbType.Char, inferredDbType: DbType.String, isDefault: false);

    //todo: 01p01: Extension is not a secure feature, and it may cause unexpected error, Set enable extension to true to use it
    /*[Test]
    [NonParallelizable]
    public async Task Citext_as_string()
    {
        await using var conn = await OpenConnectionAsync();
        await EnsureExtensionAsync(conn, "citext");
        await AssertType("foo", "foo", "citext", GaussDBDbType.Citext, inferredDbType: DbType.String, isDefaultForWriting: false);
    }*/

    [Test]
    public Task Text_as_MemoryStream()
        => AssertTypeWrite(() => new MemoryStream("foo"u8.ToArray()), "foo", "text", GaussDBDbType.Text, DbType.String, isDefault: false);

    [Test]
    public async Task Text_long()
    {
        await using var conn = await OpenConnectionAsync();
        var builder = new StringBuilder("ABCDEééé", conn.Settings.WriteBufferSize);
        builder.Append('X', conn.Settings.WriteBufferSize);
        var value = builder.ToString();

        await AssertType(value, value, "text", GaussDBDbType.Text, DbType.String);
    }

    [Test, Description("Tests that strings are truncated when the GaussDBParameter's Size is set")]
    public async Task Truncate()
    {
        const string data = "SomeText";
        using var conn = await OpenConnectionAsync();
        using var cmd = new GaussDBCommand("SELECT @p::TEXT", conn);
        var p = new GaussDBParameter("p", data) { Size = 4 };
        cmd.Parameters.Add(p);
        Assert.That(await cmd.ExecuteScalarAsync(), Is.EqualTo(data.Substring(0, 4)));

        // GaussDBParameter.Size needs to persist when value is changed
        const string data2 = "AnotherValue";
        p.Value = data2;
        Assert.That(await cmd.ExecuteScalarAsync(), Is.EqualTo(data2.Substring(0, 4)));

        // GaussDBParameter.Size larger than the value size should mean the value size, as well as 0 and -1
        p.Value = data2;
        p.Size = data2.Length + 10;
        Assert.That(await cmd.ExecuteScalarAsync(), Is.EqualTo(data2));
        p.Size = 0;
        Assert.That(await cmd.ExecuteScalarAsync(), Is.EqualTo(data2));
        p.Size = -1;
        Assert.That(await cmd.ExecuteScalarAsync(), Is.EqualTo(data2));

        Assert.That(() => p.Size = -2, Throws.Exception.TypeOf<ArgumentException>());
    }

    //todo: 目前判断该示例不适用
    /*[Test, IssueLink("https://github.com/npgsql/npgsql/issues/488")]
    public async Task Null_character()
    {
        var exception = await AssertTypeUnsupportedWrite<string, PostgresException>("string with \0\0\0 null \0bytes");
        Assert.That(exception.SqlState, Is.EqualTo(PostgresErrorCodes.CharacterNotInRepertoire));
    }*/

    [Test, Description("Tests some types which are aliased to strings")]
    [TestCase("character varying", GaussDBDbType.Varchar)]
    [TestCase("name", GaussDBDbType.Name)]
    public Task Aliased_postgres_types(string pgTypeName, GaussDBDbType gaussdbDbType)
        => AssertType("foo", "foo", pgTypeName, gaussdbDbType, inferredDbType: DbType.String, isDefaultForWriting: false);

    [Test]
    [TestCase(DbType.AnsiString)]
    [TestCase(DbType.AnsiStringFixedLength)]
    public async Task Aliased_DbTypes(DbType dbType)
    {
        await using var conn = await OpenConnectionAsync();
        await using var command = new GaussDBCommand("SELECT @p", conn);
        command.Parameters.Add(new GaussDBParameter("p", dbType) { Value = "SomeString" });
        Assert.That(await command.ExecuteScalarAsync(), Is.EqualTo("SomeString")); // Inferred DbType...
    }

    [Test, Description("Tests the PostgreSQL internal \"char\" type")]
    public async Task Internal_char()
    {
        using var conn = await OpenConnectionAsync();
        using var cmd = conn.CreateCommand();
        var testArr = new byte[] { (byte)'}', (byte)'"', 3 };
        var testArr2 = new char[] { '}', '"', (char)3 };

        cmd.CommandText = "Select 'a'::\"char\", (-3)::\"char\", :p1, :p2, :p3, :p4, :p5";
        cmd.Parameters.Add(new GaussDBParameter("p1", GaussDBDbType.InternalChar) { Value = 'b' });
        cmd.Parameters.Add(new GaussDBParameter("p2", GaussDBDbType.InternalChar) { Value = (byte)66 });
        cmd.Parameters.Add(new GaussDBParameter("p3", GaussDBDbType.InternalChar) { Value = (byte)230 });
        cmd.Parameters.Add(new GaussDBParameter("p4", GaussDBDbType.InternalChar | GaussDBDbType.Array) { Value = testArr });
        cmd.Parameters.Add(new GaussDBParameter("p5", GaussDBDbType.InternalChar | GaussDBDbType.Array) { Value = testArr2 });
        using var reader = await cmd.ExecuteReaderAsync();
        reader.Read();
        var expected = new char[] { 'a', (char)(256 - 3), 'b', (char)66, (char)230 };
        for (var i = 0; i < expected.Length; i++)
        {
            Assert.AreEqual(expected[i], reader.GetChar(i));
        }
        var arr = (char[])reader.GetValue(5);
        var arr2 = (char[])reader.GetValue(6);
        Assert.AreEqual(testArr.Length, arr.Length);
        for (var i = 0; i < arr.Length; i++)
        {
            Assert.AreEqual(testArr[i], arr[i]);
            Assert.AreEqual(testArr2[i], arr2[i]);
        }
    }

    [Test, Description("Tests VARCHAR2 and NVARCHAR2 column types with table creation and insert/query operations")]
    public async Task Varchar2_and_nvarchar2_table_operations()
    {
        await using var conn = await OpenConnectionAsync();
        var tableName = await GetTempTableName(conn);

        try
        {
            // Create table with VARCHAR2 and NVARCHAR2 columns
            await conn.ExecuteNonQueryAsync($@"
                CREATE TABLE {tableName} (
                    id INT PRIMARY KEY,
                    name VARCHAR2(100),
                    description NVARCHAR2(200)
                )");

            // Insert data
            await using (var cmd = new GaussDBCommand($@"
                INSERT INTO {tableName} (id, name, description)
                VALUES (@id, @name, @description)", conn))
            {
                cmd.Parameters.AddWithValue("id", 1);
                cmd.Parameters.AddWithValue("name", "Test Name");
                cmd.Parameters.AddWithValue("description", "Test Description");
                await cmd.ExecuteNonQueryAsync();
            }

            // Insert data with longer strings
            await using (var cmd = new GaussDBCommand($@"
                INSERT INTO {tableName} (id, name, description)
                VALUES (@id, @name, @description)", conn))
            {
                cmd.Parameters.AddWithValue("id", 2);
                cmd.Parameters.AddWithValue("name", "Another Test Name");
                cmd.Parameters.AddWithValue("description", "A much longer description to test NVARCHAR2 column handling");
                await cmd.ExecuteNonQueryAsync();
            }

            // Query data and verify
            await using (var cmd = new GaussDBCommand($"SELECT id, name, description FROM {tableName} ORDER BY id", conn))
            await using (var reader = await cmd.ExecuteReaderAsync())
            {
                // First row
                Assert.That(await reader.ReadAsync(), Is.True);
                Assert.That(reader.GetInt32(0), Is.EqualTo(1));
                Assert.That(reader.GetString(1), Is.EqualTo("Test Name"));
                Assert.That(reader.GetString(2), Is.EqualTo("Test Description"));

                // Second row
                Assert.That(await reader.ReadAsync(), Is.True);
                Assert.That(reader.GetInt32(0), Is.EqualTo(2));
                Assert.That(reader.GetString(1), Is.EqualTo("Another Test Name"));
                Assert.That(reader.GetString(2), Is.EqualTo("A much longer description to test NVARCHAR2 column handling"));

                Assert.That(await reader.ReadAsync(), Is.False);
            }

            // Test NULL values
            await using (var cmd = new GaussDBCommand($@"
                INSERT INTO {tableName} (id, name, description)
                VALUES (@id, @name, @description)", conn))
            {
                cmd.Parameters.AddWithValue("id", 3);
                cmd.Parameters.AddWithValue("name", DBNull.Value);
                cmd.Parameters.AddWithValue("description", DBNull.Value);
                await cmd.ExecuteNonQueryAsync();
            }

            // Verify NULL values
            await using (var cmd = new GaussDBCommand($"SELECT name, description FROM {tableName} WHERE id = 3", conn))
            await using (var reader = await cmd.ExecuteReaderAsync())
            {
                Assert.That(await reader.ReadAsync(), Is.True);
                Assert.That(reader.IsDBNull(0), Is.True);
                Assert.That(reader.IsDBNull(1), Is.True);
            }
        }
        finally
        {
            // Clean up
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }
}
