using System.Threading.Tasks;
using HuaweiCloud.GaussDBTypes;
using NUnit.Framework;

namespace HuaweiCloud.GaussDB.Tests.Types;

public class InternalTypeTests(MultiplexingMode multiplexingMode) : MultiplexingTestBase(multiplexingMode)
{
    [Test]
    public async Task Read_internal_char()
    {
        using var conn = await OpenConnectionAsync();
        using var cmd = new GaussDBCommand("SELECT typdelim FROM pg_type WHERE typname='int4'", conn);
        using var reader = await cmd.ExecuteReaderAsync();
        reader.Read();
        Assert.That(reader.GetChar(0), Is.EqualTo(','));
        Assert.That(reader.GetValue(0), Is.EqualTo(','));
        Assert.That(reader.GetProviderSpecificValue(0), Is.EqualTo(','));
        Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(char)));
    }

    [Test]
    [TestCase(GaussDBDbType.Oid)]
    [TestCase(GaussDBDbType.Regtype)]
    [TestCase(GaussDBDbType.Regconfig)]
    public async Task Internal_uint_types(GaussDBDbType gaussdbDbType)
    {
        var postgresType = gaussdbDbType.ToString().ToLowerInvariant();
        using var conn = await OpenConnectionAsync();
        using var cmd = new GaussDBCommand($"SELECT @max, 4294967295::{postgresType}, @eight, 8::{postgresType}", conn);
        cmd.Parameters.AddWithValue("max", gaussdbDbType, uint.MaxValue);
        cmd.Parameters.AddWithValue("eight", gaussdbDbType, 8u);
        using var reader = await cmd.ExecuteReaderAsync();
        reader.Read();

        for (var i = 0; i < reader.FieldCount; i++)
            Assert.That(reader.GetFieldType(i), Is.EqualTo(typeof(uint)));

        Assert.That(reader.GetValue(0), Is.EqualTo(uint.MaxValue));
        Assert.That(reader.GetValue(1), Is.EqualTo(uint.MaxValue));
        Assert.That(reader.GetValue(2), Is.EqualTo(8u));
        Assert.That(reader.GetValue(3), Is.EqualTo(8u));
    }

    [Test]
    public async Task Tid()
    {
        var expected = new GaussDBTid(3, 5);
        using var conn = await OpenConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT '(1234,40000)'::tid, @p::tid";
        cmd.Parameters.AddWithValue("p", GaussDBDbType.Tid, expected);
        using var reader = await cmd.ExecuteReaderAsync();
        reader.Read();
        Assert.AreEqual(1234, reader.GetFieldValue<GaussDBTid>(0).BlockNumber);
        Assert.AreEqual(40000, reader.GetFieldValue<GaussDBTid>(0).OffsetNumber);
        Assert.AreEqual(expected.BlockNumber, reader.GetFieldValue<GaussDBTid>(1).BlockNumber);
        Assert.AreEqual(expected.OffsetNumber, reader.GetFieldValue<GaussDBTid>(1).OffsetNumber);
    }

    #region GaussDBLogSequenceNumber / PgLsn

    static readonly TestCaseData[] EqualsObjectCases =
    [
        new TestCaseData(new GaussDBLogSequenceNumber(1ul), null).Returns(false),
        new TestCaseData(new GaussDBLogSequenceNumber(1ul), new object()).Returns(false),
        new TestCaseData(new GaussDBLogSequenceNumber(1ul), 1ul).Returns(false), // no implicit cast
        new TestCaseData(new GaussDBLogSequenceNumber(1ul), "0/0").Returns(false), // no implicit cast/parsing
        new TestCaseData(new GaussDBLogSequenceNumber(1ul), new GaussDBLogSequenceNumber(1ul)).Returns(true)
    ];

    [Test, TestCaseSource(nameof(EqualsObjectCases))]
    public bool GaussDBLogSequenceNumber_equals(GaussDBLogSequenceNumber lsn, object? obj)
        => lsn.Equals(obj);


    //todo: 不支持GaussDBDbType PgLsn类型
    /*[Test]
    public async Task GaussDBLogSequenceNumber()
    {
        var expected1 = new GaussDBLogSequenceNumber(42949672971ul);
        Assert.AreEqual(expected1, GaussDBTypes.GaussDBLogSequenceNumber.Parse("A/B"));
        await using var conn = await OpenConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 'A/B'::pg_lsn, @p::pg_lsn";
        cmd.Parameters.AddWithValue("p", GaussDBDbType.PgLsn, expected1);
        await using var reader = await cmd.ExecuteReaderAsync();
        reader.Read();
        var result1 = reader.GetFieldValue<GaussDBLogSequenceNumber>(0);
        var result2 = reader.GetFieldValue<GaussDBLogSequenceNumber>(1);
        Assert.AreEqual(expected1, result1);
        Assert.AreEqual(42949672971ul, (ulong)result1);
        Assert.AreEqual("A/B", result1.ToString());
        Assert.AreEqual(expected1, result2);
        Assert.AreEqual(42949672971ul, (ulong)result2);
        Assert.AreEqual("A/B", result2.ToString());
    }*/

    #endregion GaussDBLogSequenceNumber / PgLsn
}
