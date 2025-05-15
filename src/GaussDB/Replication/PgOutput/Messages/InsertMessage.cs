using GaussDBTypes;
using System;
using System.Threading;
using System.Threading.Tasks;
using GaussDB.Internal;

namespace GaussDB.Replication.PgOutput.Messages;

/// <summary>
/// Logical Replication Protocol insert message
/// </summary>
public sealed class InsertMessage : TransactionalMessage
{
    readonly ReplicationTuple _tupleEnumerable;

    /// <summary>
    /// The relation for this <see cref="InsertMessage" />.
    /// </summary>
    public RelationMessage Relation { get; private set; } = null!;

    /// <summary>
    /// Columns representing the new row.
    /// </summary>
    public ReplicationTuple NewRow => _tupleEnumerable;

    internal InsertMessage(GaussDBConnector connector)
        => _tupleEnumerable = new(connector);

    internal InsertMessage Populate(
        GaussDBLogSequenceNumber walStart, GaussDBLogSequenceNumber walEnd, DateTime serverClock, uint? transactionXid,
        RelationMessage relation, ushort numColumns)
    {
        base.Populate(walStart, walEnd, serverClock, transactionXid);

        Relation = relation;
        _tupleEnumerable.Reset(numColumns, relation.RowDescription);

        return this;
    }

    internal Task Consume(CancellationToken cancellationToken)
        => _tupleEnumerable.Consume(cancellationToken);
}
