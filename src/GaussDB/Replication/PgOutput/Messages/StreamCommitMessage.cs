using HuaweiCloud.GaussDBTypes;
using System;

namespace HuaweiCloud.GaussDB.Replication.PgOutput.Messages;

/// <summary>
/// Logical Replication Protocol stream commit message
/// </summary>
public sealed class StreamCommitMessage : TransactionControlMessage
{
    /// <summary>
    /// Flags; currently unused (must be 0).
    /// </summary>
    public byte Flags { get; private set; }

    /// <summary>
    /// The LSN of the commit.
    /// </summary>
    public GaussDBLogSequenceNumber CommitLsn { get; private set; }

    /// <summary>
    /// The end LSN of the transaction.
    /// </summary>
    public GaussDBLogSequenceNumber TransactionEndLsn { get; private set; }

    /// <summary>
    /// Commit timestamp of the transaction.
    /// </summary>
    public DateTime TransactionCommitTimestamp { get; private set; }

    internal StreamCommitMessage() {}

    internal StreamCommitMessage Populate(GaussDBLogSequenceNumber walStart, GaussDBLogSequenceNumber walEnd, DateTime serverClock,
        uint transactionXid, byte flags, GaussDBLogSequenceNumber commitLsn, GaussDBLogSequenceNumber transactionEndLsn, DateTime transactionCommitTimestamp)
    {
        base.Populate(walStart, walEnd, serverClock, transactionXid);
        Flags = flags;
        CommitLsn = commitLsn;
        TransactionEndLsn = transactionEndLsn;
        TransactionCommitTimestamp = transactionCommitTimestamp;
        return this;
    }
}
