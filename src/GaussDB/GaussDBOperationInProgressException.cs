using HuaweiCloud.GaussDB.Internal;

namespace HuaweiCloud.GaussDB;

/// <summary>
/// Thrown when trying to use a connection that is already busy performing some other operation.
/// Provides information on the already-executing operation to help with debugging.
/// </summary>
public sealed class GaussDBOperationInProgressException : GaussDBException
{
    /// <summary>
    /// Creates a new instance of <see cref="GaussDBOperationInProgressException" />.
    /// </summary>
    /// <param name="command">
    /// A command which was in progress when the operation which triggered this exception was executed.
    /// </param>
    public GaussDBOperationInProgressException(GaussDBCommand command)
        : base("A command is already in progress: " + command.CommandText)
        => CommandInProgress = command;

    internal GaussDBOperationInProgressException(ConnectorState state)
        : base($"The connection is already in state '{state}'")
    {
    }

    /// <summary>
    /// If the connection is busy with another command, this will contain a reference to that command.
    /// Otherwise, if the connection if busy with another type of operation (e.g. COPY), contains
    /// <see langword="null" />.
    /// </summary>
    public GaussDBCommand? CommandInProgress { get; }
}
