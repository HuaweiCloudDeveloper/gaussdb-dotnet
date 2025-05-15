using HuaweiCloud.GaussDBTypes;
using System;

namespace HuaweiCloud.GaussDB.Replication.PgOutput.Messages;

/// <summary>
/// Logical Replication Protocol begin prepare message
/// </summary>
public sealed class BeginPrepareMessage : PrepareMessageBase
{
    internal BeginPrepareMessage() {}

    internal new BeginPrepareMessage Populate(
        GaussDBLogSequenceNumber walStart, GaussDBLogSequenceNumber walEnd, DateTime serverClock,
        GaussDBLogSequenceNumber prepareLsn, GaussDBLogSequenceNumber prepareEndLsn, DateTime transactionPrepareTimestamp,
        uint transactionXid, string transactionGid)
    {
        base.Populate(walStart, walEnd, serverClock,
            prepareLsn: prepareLsn,
            prepareEndLsn: prepareEndLsn,
            transactionPrepareTimestamp: transactionPrepareTimestamp,
            transactionXid: transactionXid,
            transactionGid: transactionGid);
        return this;
    }
}

