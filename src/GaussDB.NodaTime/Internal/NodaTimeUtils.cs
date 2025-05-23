using System;
using NodaTime;
using HuaweiCloud.GaussDB.NodaTime.Properties;

namespace HuaweiCloud.GaussDB.NodaTime.Internal;

static class NodaTimeUtils
{
#if DEBUG
    internal static bool LegacyTimestampBehavior;
#else
    internal static readonly bool LegacyTimestampBehavior;
#endif

    static NodaTimeUtils() => LegacyTimestampBehavior = AppContext.TryGetSwitch("GaussDB.EnableLegacyTimestampBehavior", out var enabled) && enabled;

    static readonly Instant Instant2000 = Instant.FromUtc(2000, 1, 1, 0, 0, 0);
    static readonly Duration Plus292Years = Duration.FromDays(292 * 365);
    static readonly Duration Minus292Years = -Plus292Years;

    /// <summary>
    /// Decodes a PostgreSQL timestamp/timestamptz into a NodaTime Instant.
    /// </summary>
    /// <param name="value">The number of microseconds from 2000-01-01T00:00:00.</param>
    /// <param name="dateTimeInfinityConversions">Whether infinity date/time conversions are enabled.</param>
    /// <remarks>
    /// Unfortunately NodaTime doesn't have Duration.FromMicroseconds(), so we decompose into milliseconds and nanoseconds.
    /// </remarks>
    internal static Instant DecodeInstant(long value, bool dateTimeInfinityConversions)
        => value switch
        {
            long.MaxValue => dateTimeInfinityConversions
                ? Instant.MaxValue
                : throw new InvalidCastException(GaussDBNodaTimeStrings.CannotReadInfinityValue),
            long.MinValue => dateTimeInfinityConversions
                ? Instant.MinValue
                : throw new InvalidCastException(GaussDBNodaTimeStrings.CannotReadInfinityValue),
            _ => Instant2000 + Duration.FromMilliseconds(value / 1000) + Duration.FromNanoseconds(value % 1000 * 1000)
        };

    /// <summary>
    /// Encodes a NodaTime Instant to a PostgreSQL timestamp/timestamptz.
    /// </summary>
    internal static long EncodeInstant(Instant instant, bool dateTimeInfinityConversions)
    {
        if (dateTimeInfinityConversions)
        {
            if (instant == Instant.MaxValue)
                return long.MaxValue;

            if (instant == Instant.MinValue)
                return long.MinValue;
        }

        // We need to write the number of microseconds from 2000-01-01T00:00:00.
        var since2000 = instant - Instant2000;

        // The nanoseconds may overflow, so fallback to BigInteger where necessary.
        return since2000 >= Minus292Years && since2000 <= Plus292Years
            ? since2000.ToInt64Nanoseconds() / 1000
            : (long)(since2000.ToBigIntegerNanoseconds() / 1000);
    }
}
