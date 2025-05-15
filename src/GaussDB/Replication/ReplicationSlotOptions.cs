using System;
using HuaweiCloud.GaussDBTypes;

namespace HuaweiCloud.GaussDB.Replication;

/// <summary>
/// Contains information about a replication slot.
/// </summary>
public readonly struct ReplicationSlotOptions
{
    /// <summary>
    /// Creates a new <see cref="ReplicationSlotOptions"/> instance.
    /// </summary>
    /// <param name="slotName">
    /// The name of the replication slot.
    /// </param>
    /// <param name="consistentPoint">
    /// The WAL location at which the slot became consistent.
    /// </param>
    public ReplicationSlotOptions(string slotName, string? consistentPoint = null)
        : this(slotName, consistentPoint is null ? default : GaussDBLogSequenceNumber.Parse(consistentPoint), null){}

    /// <summary>
    /// Creates a new <see cref="ReplicationSlotOptions"/> instance.
    /// </summary>
    /// <param name="slotName">
    /// The name of the replication slot.
    /// </param>
    /// <param name="consistentPoint">
    /// The WAL location at which the slot became consistent.
    /// </param>
    public ReplicationSlotOptions(string slotName, GaussDBLogSequenceNumber consistentPoint)
        : this(slotName, consistentPoint, null) {}

    internal ReplicationSlotOptions(
        string slotName,
        GaussDBLogSequenceNumber consistentPoint,
        string? snapshotName)
    {
        SlotName = slotName ?? throw new ArgumentNullException(nameof(slotName), "The replication slot name cannot be null.");
        ConsistentPoint = consistentPoint;
        SnapshotName = snapshotName;
    }

    /// <summary>
    /// The name of the replication slot.
    /// </summary>
    public string SlotName { get; }

    /// <summary>
    /// The WAL location at which the slot became consistent.
    /// </summary>
    public GaussDBLogSequenceNumber ConsistentPoint { get; }

    /// <summary>
    /// The identifier of the snapshot exported by the CREATE_REPLICATION_SLOT command.
    /// </summary>
    internal string? SnapshotName { get; }
}
