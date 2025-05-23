using System;
using System.Data;
using System.Threading.Tasks;
using HuaweiCloud.GaussDB.Internal.ResolverFactories;
using HuaweiCloud.GaussDBTypes;
using NUnit.Framework;
using static HuaweiCloud.GaussDB.Util.Statics;

namespace HuaweiCloud.GaussDB.Tests.Types;

// Since this test suite manipulates TimeZone, it is incompatible with multiplexing
[NonParallelizable]
public class LegacyDateTimeTests : TestBase
{
    [Test]
    public Task Timestamp_with_all_DateTime_kinds([Values] DateTimeKind kind)
        => AssertType(
            new DateTime(1998, 4, 12, 13, 26, 38, 789, kind),
            "1998-04-12 13:26:38.789",
            "timestamp without time zone",
            GaussDBDbType.Timestamp,
            DbType.DateTime);

    [Test]
    public async Task Timestamp_read_as_Unspecified_DateTime()
    {
        await using var command = DataSource.CreateCommand("SELECT '2020-03-01T10:30:00'::timestamp");
        var dateTime = (DateTime)(await command.ExecuteScalarAsync())!;
        Assert.That(dateTime.Kind, Is.EqualTo(DateTimeKind.Unspecified));
    }

    [Test]
    public async Task Timestamptz_negative_infinity()
    {
        var dto = await AssertType(DateTimeOffset.MinValue, "-infinity", "timestamp with time zone", GaussDBDbType.TimestampTz,
            DbType.DateTimeOffset, isDefaultForReading: false);
        Assert.That(dto.Offset, Is.EqualTo(TimeSpan.Zero));
    }

    [Test]
    public async Task Timestamptz_infinity()
    {
        var dto = await AssertType(
            DateTimeOffset.MaxValue, "infinity", "timestamp with time zone", GaussDBDbType.TimestampTz, DbType.DateTimeOffset,
            isDefaultForReading: false);
        Assert.That(dto.Offset, Is.EqualTo(TimeSpan.Zero));
    }

    [Test]
    [TestCase(DateTimeKind.Utc, TestName = "Timestamptz_write_utc_DateTime_does_not_convert")]
    [TestCase(DateTimeKind.Unspecified, TestName = "Timestamptz_write_unspecified_DateTime_does_not_convert")]
    public Task Timestamptz_write_utc_DateTime_does_not_convert(DateTimeKind kind)
        => AssertTypeWrite(
            new DateTime(1998, 4, 12, 13, 26, 38, 789, kind),
            "1998-04-12 15:26:38.789+02",
            "timestamp with time zone",
            GaussDBDbType.TimestampTz,
            DbType.DateTimeOffset,
            isDefault: false);

    [Test]
    public Task Timestamptz_local_DateTime_converts()
    {
        // In legacy mode, we convert local DateTime to UTC when writing, and convert to local when reading,
        // using the machine time zone.
        var dateTime = new DateTime(1998, 4, 12, 13, 26, 38, 789, DateTimeKind.Utc).ToLocalTime();

        return AssertType(
            dateTime,
            "1998-04-12 15:26:38.789+02",
            "timestamp with time zone",
            GaussDBDbType.TimestampTz,
            DbType.DateTimeOffset,
            isDefaultForWriting: false);
    }

    GaussDBDataSource _dataSource = null!;
    protected override GaussDBDataSource DataSource => _dataSource;

    [OneTimeSetUp]
    public void Setup()
    {
#if DEBUG
        LegacyTimestampBehavior = true;
        _dataSource = CreateDataSource(builder =>
        {
            // Can't use the static AdoTypeInfoResolver instance, it already captured the feature flag.
            builder.AddTypeInfoResolverFactory(new AdoTypeInfoResolverFactory());
            builder.ConnectionStringBuilder.Timezone = "Europe/Berlin";
        });
        GaussDBDataSourceBuilder.ResetGlobalMappings(overwrite: true);
#else
        Assert.Ignore(
            "Legacy DateTime tests rely on the GaussDB.EnableLegacyTimestampBehavior AppContext switch and can only be run in DEBUG builds");
#endif
    }

#if DEBUG
    [OneTimeTearDown]
    public void Teardown()
    {
        LegacyTimestampBehavior = false;
        _dataSource.Dispose();
        GaussDBDataSourceBuilder.ResetGlobalMappings(overwrite: true);
    }
#endif
}
