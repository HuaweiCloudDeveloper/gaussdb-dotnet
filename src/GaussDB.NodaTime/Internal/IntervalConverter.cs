using System.Threading;
using System.Threading.Tasks;
using NodaTime;
using HuaweiCloud.GaussDB.Internal;
using HuaweiCloud.GaussDBTypes;

namespace HuaweiCloud.GaussDB.NodaTime.Internal;

public class IntervalConverter(PgConverter<GaussDBRange<Instant>> rangeConverter) : PgStreamingConverter<Interval>
{
    public override Interval Read(PgReader reader)
        => Read(async: false, reader, CancellationToken.None).GetAwaiter().GetResult();

    public override ValueTask<Interval> ReadAsync(PgReader reader, CancellationToken cancellationToken = default)
        => Read(async: true, reader, cancellationToken);

    async ValueTask<Interval> Read(bool async, PgReader reader, CancellationToken cancellationToken)
    {
        var range = async
            ? await rangeConverter.ReadAsync(reader, cancellationToken).ConfigureAwait(false)
            // ReSharper disable once MethodHasAsyncOverloadWithCancellation
            : rangeConverter.Read(reader);

        // NodaTime Interval includes the start instant and excludes the end instant.
        Instant? start = range.LowerBoundInfinite
            ? null
            : range.LowerBoundIsInclusive
                ? range.LowerBound
                : range.LowerBound + Duration.Epsilon;
        Instant? end = range.UpperBoundInfinite
            ? null
            : range.UpperBoundIsInclusive
                ? range.UpperBound + Duration.Epsilon
                : range.UpperBound;

        return new(start, end);
    }

    public override Size GetSize(SizeContext context, Interval value, ref object? writeState)
        => rangeConverter.GetSize(context, IntervalToGaussDBRange(value), ref writeState);

    public override void Write(PgWriter writer, Interval value)
        => rangeConverter.Write(writer, IntervalToGaussDBRange(value));

    public override ValueTask WriteAsync(PgWriter writer, Interval value, CancellationToken cancellationToken = default)
        => rangeConverter.WriteAsync(writer, IntervalToGaussDBRange(value), cancellationToken);

    static GaussDBRange<Instant> IntervalToGaussDBRange(Interval interval)
        => new(
            interval.HasStart ? interval.Start : default, true, !interval.HasStart,
            interval.HasEnd ? interval.End : default, false, !interval.HasEnd);
}
