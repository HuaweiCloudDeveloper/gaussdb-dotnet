using HuaweiCloud.GaussDBTypes;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace HuaweiCloud.GaussDB.Replication.Internal;

/// <summary>
/// This API is for internal use and for implementing logical replication plugins.
/// It is not meant to be consumed in common GaussDB usage scenarios.
/// </summary>
public static class LogicalReplicationConnectionExtensions
{
    /// <summary>
    /// This API is for internal use and for implementing logical replication plugins.
    /// It is not meant to be consumed in common GaussDB usage scenarios.
    /// </summary>
    /// <remarks>
    /// Creates a new replication slot and returns information about the newly-created slot.
    /// </remarks>
    /// <param name="connection">The <see cref="LogicalReplicationConnection"/> to use for creating the
    /// replication slot</param>
    /// <param name="slotName">The name of the slot to create. Must be a valid replication slot name (see
    /// <a href="https://www.postgresql.org/docs/current/warm-standby.html#STREAMING-REPLICATION-SLOTS-MANIPULATION">
    /// https://www.postgresql.org/docs/current/warm-standby.html#STREAMING-REPLICATION-SLOTS-MANIPULATION</a>).
    /// </param>
    /// <param name="outputPlugin">The name of the output plugin used for logical decoding (see
    /// <a href="https://www.postgresql.org/docs/current/logicaldecoding-output-plugin.html">
    /// https://www.postgresql.org/docs/current/logicaldecoding-output-plugin.html</a>).
    /// </param>
    /// <param name="isTemporary"><see langword="true"/> if this replication slot shall be temporary one; otherwise
    /// <see langword="false"/>. Temporary slots are not saved to disk and are automatically dropped on error or
    /// when the session has finished.</param>
    /// <param name="slotSnapshotInitMode">A <see cref="LogicalSlotSnapshotInitMode"/> to specify what to do with the
    /// snapshot created during logical slot initialization. <see cref="LogicalSlotSnapshotInitMode.Export"/>, which is
    /// also the default, will export the snapshot for use in other sessions. This option can't be used inside a
    /// transaction. <see cref="LogicalSlotSnapshotInitMode.Use"/> will use the snapshot for the current transaction
    /// executing the command. This option must be used in a transaction, and <see cref="LogicalSlotSnapshotInitMode.Use"/>
    /// must be the first command run in that transaction. Finally, <see cref="LogicalSlotSnapshotInitMode.NoExport"/> will
    /// just use the snapshot for logical decoding as normal but won't do anything else with it.</param>
    /// <param name="twoPhase">
    /// If <see langword="true"/>, this logical replication slot supports decoding of two-phase transactions. With this option,
    /// two-phase commands like PREPARE TRANSACTION, COMMIT PREPARED and ROLLBACK PREPARED are decoded and transmitted.
    /// The transaction will be decoded and transmitted at PREPARE TRANSACTION time. The default is <see langword="false"/>.
    /// </param>
    /// <param name="cancellationToken">
    /// An optional token to cancel the asynchronous operation. The default value is <see cref="CancellationToken.None"/>.
    /// </param>
    /// <returns>A <see cref="Task{T}"/> representing a <see cref="ReplicationSlotOptions"/> class that
    /// can be used to initialize instances of <see cref="ReplicationSlot"/> subclasses.</returns>
    public static Task<ReplicationSlotOptions> CreateLogicalReplicationSlot(
        this LogicalReplicationConnection connection,
        string slotName,
        string outputPlugin,
        bool isTemporary = false,
        LogicalSlotSnapshotInitMode? slotSnapshotInitMode = null,
        bool twoPhase = false,
        CancellationToken cancellationToken = default)
    {
        connection.CheckDisposed();
        ArgumentNullException.ThrowIfNull(slotName);
        ArgumentNullException.ThrowIfNull(outputPlugin);

        cancellationToken.ThrowIfCancellationRequested();

        var builder = new StringBuilder("CREATE_REPLICATION_SLOT ").Append(slotName);
        if (isTemporary)
            builder.Append(" TEMPORARY");
        builder.Append(" LOGICAL ").Append(outputPlugin);
        if (connection.PostgreSqlVersion.Major >= 15 && (slotSnapshotInitMode.HasValue || twoPhase))
        {
            builder.Append('(');
            if (slotSnapshotInitMode.HasValue)
            {
                builder.Append(slotSnapshotInitMode switch
                {
                    LogicalSlotSnapshotInitMode.Export => "SNAPSHOT 'export'",
                    LogicalSlotSnapshotInitMode.Use => "SNAPSHOT 'use'",
                    LogicalSlotSnapshotInitMode.NoExport => "SNAPSHOT 'nothing'",
                    _ => throw new ArgumentOutOfRangeException(nameof(slotSnapshotInitMode),
                        slotSnapshotInitMode,
                        $"Unexpected value {slotSnapshotInitMode} for argument {nameof(slotSnapshotInitMode)}.")
                });
                if (twoPhase)
                    builder.Append(",TWO_PHASE");
            }
            else
                builder.Append("TWO_PHASE");
            builder.Append(')');
        }
        else
        {
            builder.Append(slotSnapshotInitMode switch
            {
                // EXPORT_SNAPSHOT is the default since it has been introduced.
                // We don't set it unless it is explicitly requested so that older backends can digest the query too.
                null => string.Empty,
                LogicalSlotSnapshotInitMode.Export => " EXPORT_SNAPSHOT",
                LogicalSlotSnapshotInitMode.Use => " USE_SNAPSHOT",
                LogicalSlotSnapshotInitMode.NoExport => " NOEXPORT_SNAPSHOT",
                _ => throw new ArgumentOutOfRangeException(nameof(slotSnapshotInitMode),
                    slotSnapshotInitMode,
                    $"Unexpected value {slotSnapshotInitMode} for argument {nameof(slotSnapshotInitMode)}.")
            });
            if (twoPhase)
                builder.Append(" TWO_PHASE");
        }
        var command = builder.ToString();

        LogMessages.CreatingReplicationSlot(connection.ReplicationLogger, slotName, command, connection.Connector.Id);

        return connection.CreateReplicationSlot(command, cancellationToken);
    }

    /// <summary>
    /// Instructs the server to start streaming the WAL for logical replication, starting at WAL location
    /// <paramref name="walLocation"/> or at the slot's consistent point if <paramref name="walLocation"/> isn't specified.
    /// The server can reply with an error, for example if the requested section of the WAL has already been recycled.
    /// </summary>
    /// <param name="connection">The <see cref="LogicalReplicationConnection"/> to use for starting replication</param>
    /// <param name="slot">The replication slot that will be updated as replication progresses so that the server
    /// knows which WAL segments are still needed by the standby.
    /// </param>
    /// <param name="cancellationToken">The token to monitor for stopping the replication.</param>
    /// <param name="walLocation">The WAL location to begin streaming at.</param>
    /// <param name="options">The collection of options passed to the slot's logical decoding plugin.</param>
    /// <param name="bypassingStream">
    /// Whether the plugin will be bypassing <see cref="XLogDataMessage.Data" /> and reading directly from the buffer.
    /// </param>
    /// <returns>A <see cref="Task{T}"/> representing an <see cref="IAsyncEnumerable{T}"/> that
    /// can be used to stream WAL entries in form of <see cref="XLogDataMessage"/> instances.</returns>
    public static IAsyncEnumerable<XLogDataMessage> StartLogicalReplication(
        this LogicalReplicationConnection connection,
        LogicalReplicationSlot slot,
        CancellationToken cancellationToken,
        GaussDBLogSequenceNumber? walLocation = null,
        IEnumerable<KeyValuePair<string, string?>>? options = null,
        bool bypassingStream = false)
    {
        return StartLogicalReplicationInternal(connection, slot, cancellationToken, walLocation, options, bypassingStream);

        // Local method to avoid having to add the EnumeratorCancellation attribute to the public signature.
        static async IAsyncEnumerable<XLogDataMessage> StartLogicalReplicationInternal(
            LogicalReplicationConnection connection,
            LogicalReplicationSlot slot,
            [EnumeratorCancellation] CancellationToken cancellationToken,
            GaussDBLogSequenceNumber? walLocation,
            IEnumerable<KeyValuePair<string, string?>>? options,
            bool bypassingStream)
        {
            var builder = new StringBuilder("START_REPLICATION ")
                .Append("SLOT ").Append(slot.Name)
                .Append(" LOGICAL ")
                .Append(walLocation ?? slot.ConsistentPoint);

            var opts = new List<KeyValuePair<string, string?>>(options ?? Array.Empty<KeyValuePair<string, string?>>());
            if (opts.Count > 0)
            {
                builder.Append(" (");
                var stringOptions = new string[opts.Count];
                for (var i = 0; i < opts.Count; i++)
                {
                    var kv = opts[i];
                    stringOptions[i] = @$"""{kv.Key}""{(kv.Value is null ? "" : $" '{kv.Value}'")}";
                }
                builder.Append(string.Join(", ", stringOptions));
                builder.Append(')');
            }

            var command = builder.ToString();

            LogMessages.StartingLogicalReplication(connection.ReplicationLogger, slot.Name, command, connection.Connector.Id);

            var enumerator = connection.StartReplicationInternalWrapper(command, bypassingStream, cancellationToken);
            while (await enumerator.MoveNextAsync().ConfigureAwait(false))
                yield return enumerator.Current;
        }
    }
}
