using GaussDBTypes;
using System;

namespace GaussDB.Replication.PgOutput.Messages;

/// <summary>
/// Abstract base class for the logical replication protocol begin prepare and prepare message
/// </summary>
public abstract class PrepareMessageBase : PreparedTransactionControlMessage
{
    /// <summary>
    /// The LSN of the prepare.
    /// </summary>
    public GaussDBLogSequenceNumber PrepareLsn => FirstLsn;

    /// <summary>
    /// The end LSN of the prepared transaction.
    /// </summary>
    public GaussDBLogSequenceNumber PrepareEndLsn => SecondLsn;

    /// <summary>
    /// Prepare timestamp of the transaction.
    /// </summary>
    public DateTime TransactionPrepareTimestamp => Timestamp;

    private protected PrepareMessageBase() {}

    internal new PrepareMessageBase Populate(
        GaussDBLogSequenceNumber walStart, GaussDBLogSequenceNumber walEnd, DateTime serverClock,
        GaussDBLogSequenceNumber prepareLsn, GaussDBLogSequenceNumber prepareEndLsn, DateTime transactionPrepareTimestamp,
        uint transactionXid, string transactionGid)
    {
        base.Populate(walStart, walEnd, serverClock,
            firstLsn: prepareLsn,
            secondLsn: prepareEndLsn,
            timestamp: transactionPrepareTimestamp,
            transactionXid: transactionXid,
            transactionGid: transactionGid);
        return this;
    }
}
