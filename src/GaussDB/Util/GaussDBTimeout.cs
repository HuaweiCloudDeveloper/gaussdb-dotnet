using System;
using System.Threading;
using HuaweiCloud.GaussDB.Internal;

namespace HuaweiCloud.GaussDB.Util;

/// <summary>
/// Represents a timeout that will expire at some point.
/// </summary>
public readonly struct GaussDBTimeout
{
    readonly DateTime _expiration;

    internal static readonly GaussDBTimeout Infinite = new(TimeSpan.Zero);

    internal GaussDBTimeout(TimeSpan expiration)
        => _expiration = expiration > TimeSpan.Zero
            ? DateTime.UtcNow + expiration
            : expiration == TimeSpan.Zero
                ? DateTime.MaxValue
                : DateTime.MinValue;

    internal void Check()
    {
        if (HasExpired)
            ThrowHelper.ThrowGaussDBExceptionWithInnerTimeoutException("The operation has timed out");
    }

    internal void CheckAndApply(GaussDBConnector connector)
    {
        if (!IsSet)
            return;

        var timeLeft = CheckAndGetTimeLeft();
        // Set the remaining timeout on the read and write buffers
        connector.ReadBuffer.Timeout = connector.WriteBuffer.Timeout = timeLeft;
    }

    internal bool IsSet => _expiration != DateTime.MaxValue;

    internal bool HasExpired => DateTime.UtcNow >= _expiration;

    internal TimeSpan CheckAndGetTimeLeft()
    {
        if (!IsSet)
            return Timeout.InfiniteTimeSpan;
        var timeLeft = _expiration - DateTime.UtcNow;
        if (timeLeft <= TimeSpan.Zero)
            Check();
        return timeLeft;
    }
}
