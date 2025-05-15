using HuaweiCloud.GaussDBTypes;
using System;

namespace HuaweiCloud.GaussDB.Replication.PgOutput.Messages;

/// <summary>
/// Logical Replication Protocol stream start message
/// </summary>
public sealed class StreamStartMessage : TransactionControlMessage
{
    /// <summary>
    /// A value of 1 indicates this is the first stream segment for this XID, 0 for any other stream segment.
    /// </summary>
    public byte StreamSegmentIndicator { get; private set; }

    internal StreamStartMessage() {}

    internal StreamStartMessage Populate(GaussDBLogSequenceNumber walStart, GaussDBLogSequenceNumber walEnd, DateTime serverClock,
        uint transactionXid, byte streamSegmentIndicator)
    {
        base.Populate(walStart, walEnd, serverClock, transactionXid);
        StreamSegmentIndicator = streamSegmentIndicator;
        return this;
    }
}
