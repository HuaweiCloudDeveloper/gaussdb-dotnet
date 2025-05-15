using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace HuaweiCloud.GaussDB;

/// <summary>
/// Configures GaussDB logging
/// </summary>
public class GaussDBLoggingConfiguration
{
    internal static readonly GaussDBLoggingConfiguration NullConfiguration
        = new(NullLoggerFactory.Instance, isParameterLoggingEnabled: false);

    internal static ILoggerFactory GlobalLoggerFactory = NullLoggerFactory.Instance;
    internal static bool GlobalIsParameterLoggingEnabled;

    internal GaussDBLoggingConfiguration(ILoggerFactory loggerFactory, bool isParameterLoggingEnabled)
    {
        ConnectionLogger = loggerFactory.CreateLogger("GaussDB.Connection");
        CommandLogger = loggerFactory.CreateLogger("GaussDB.Command");
        TransactionLogger = loggerFactory.CreateLogger("GaussDB.Transaction");
        CopyLogger = loggerFactory.CreateLogger("GaussDB.Copy");
        ReplicationLogger = loggerFactory.CreateLogger("GaussDB.Replication");
        ExceptionLogger = loggerFactory.CreateLogger("GaussDB.Exception");

        IsParameterLoggingEnabled = isParameterLoggingEnabled;
    }

    internal ILogger ConnectionLogger { get; }
    internal ILogger CommandLogger { get; }
    internal ILogger TransactionLogger { get; }
    internal ILogger CopyLogger { get; }
    internal ILogger ReplicationLogger { get; }
    internal ILogger ExceptionLogger { get; }

    /// <summary>
    /// Determines whether parameter contents will be logged alongside SQL statements - this may reveal sensitive information.
    /// Defaults to false.
    /// </summary>
    internal bool IsParameterLoggingEnabled { get; }

    /// <summary>
    /// <para>
    /// Globally initializes GaussDB logging to use the provided <paramref name="loggerFactory" />.
    /// Must be called before any GaussDB APIs are used.
    /// </para>
    /// <para>
    /// This is a legacy-only, backwards compatibility API. New applications should set the logger factory on
    /// <see cref="GaussDBDataSourceBuilder" /> and use the resulting <see cref="GaussDBDataSource "/> instead.
    /// </para>
    /// </summary>
    /// <param name="loggerFactory">The logging factory to use when logging from GaussDB.</param>
    /// <param name="parameterLoggingEnabled">
    /// Determines whether parameter contents will be logged alongside SQL statements - this may reveal sensitive information.
    /// Defaults to <see langword="false" />.
    /// </param>
    public static void InitializeLogging(ILoggerFactory loggerFactory, bool parameterLoggingEnabled = false)
        => (GlobalLoggerFactory, GlobalIsParameterLoggingEnabled) = (loggerFactory, parameterLoggingEnabled);
}
