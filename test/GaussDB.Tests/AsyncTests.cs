using NUnit.Framework;
using System.Data;
using System.Threading.Tasks;
using static GaussDB.Tests.TestUtil;

namespace GaussDB.Tests;

public class AsyncTests : TestBase
{
    [Test]
    public async Task NonQuery()
    {
        await using var conn = await OpenConnectionAsync();

        // 创建临时表（使用正确的临时表语法）
        var tableName = await CreateTempTable(conn, "int INTEGER");
        await using (var createTableCmd = conn.CreateCommand())
        {
            createTableCmd.CommandText = $"CREATE TEMP TABLE {tableName} (int INTEGER)";
            await createTableCmd.ExecuteNonQueryAsync();
        }

        // 插入数据
        await using (var insertCmd = conn.CreateCommand())
        {
            insertCmd.CommandText = $"INSERT INTO {tableName} (int) VALUES (4)";
            await insertCmd.ExecuteNonQueryAsync();
        }

        // 验证结果
        await using (var selectCmd = conn.CreateCommand())
        {
            selectCmd.CommandText = $"SELECT int FROM {tableName}";
            Assert.That(await selectCmd.ExecuteScalarAsync(), Is.EqualTo(4));
        }
    }

    [Test]
    public async Task Scalar()
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new GaussDBCommand("SELECT 1", conn);
        Assert.That(await cmd.ExecuteScalarAsync(), Is.EqualTo(1));
    }

    [Test]
    public async Task Reader()
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new GaussDBCommand("SELECT 1", conn);
        await using var reader = await cmd.ExecuteReaderAsync();
        await reader.ReadAsync();
        Assert.That(reader[0], Is.EqualTo(1));
    }

    [Test]
    public async Task Columnar()
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new GaussDBCommand("SELECT NULL, 2, 'Some Text'", conn);
        await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess);
        await reader.ReadAsync();
        Assert.That(await reader.IsDBNullAsync(0), Is.True);
        Assert.That(await reader.GetFieldValueAsync<string>(2), Is.EqualTo("Some Text"));
    }
}
