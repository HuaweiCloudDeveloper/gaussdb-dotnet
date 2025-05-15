using System;
using System.Data.Common;

namespace GaussDB;

/// <summary>
/// A factory to create instances of various GaussDB objects.
/// </summary>
[Serializable]
public sealed class GaussDBFactory : DbProviderFactory, IServiceProvider
{
    /// <summary>
    /// Gets an instance of the <see cref="GaussDBFactory"/>.
    /// This can be used to retrieve strongly typed data objects.
    /// </summary>
    public static readonly GaussDBFactory Instance = new();

    GaussDBFactory() {}

    /// <summary>
    /// Returns a strongly typed <see cref="DbCommand"/> instance.
    /// </summary>
    public override DbCommand CreateCommand() => new GaussDBCommand();

    /// <summary>
    /// Returns a strongly typed <see cref="DbConnection"/> instance.
    /// </summary>
    public override DbConnection CreateConnection() => new GaussDBConnection();

    /// <summary>
    /// Returns a strongly typed <see cref="DbParameter"/> instance.
    /// </summary>
    public override DbParameter CreateParameter() => new GaussDBParameter();

    /// <summary>
    /// Returns a strongly typed <see cref="DbConnectionStringBuilder"/> instance.
    /// </summary>
    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new GaussDBConnectionStringBuilder();

    /// <summary>
    /// Returns a strongly typed <see cref="DbCommandBuilder"/> instance.
    /// </summary>
    public override DbCommandBuilder CreateCommandBuilder() => new GaussDBCommandBuilder();

    /// <summary>
    /// Returns a strongly typed <see cref="DbDataAdapter"/> instance.
    /// </summary>
    public override DbDataAdapter CreateDataAdapter() => new GaussDBDataAdapter();

    /// <summary>
    /// Specifies whether the specific <see cref="DbProviderFactory"/> supports the <see cref="DbDataAdapter"/> class.
    /// </summary>
    public override bool CanCreateDataAdapter => true;

    /// <summary>
    /// Specifies whether the specific <see cref="DbProviderFactory"/> supports the <see cref="DbCommandBuilder"/> class.
    /// </summary>
    public override bool CanCreateCommandBuilder => true;

    /// <inheritdoc/>
    public override bool CanCreateBatch => true;

    /// <inheritdoc/>
    public override DbBatch CreateBatch() => new GaussDBBatch();

    /// <inheritdoc/>
    public override DbBatchCommand CreateBatchCommand() => new GaussDBBatchCommand();

    /// <inheritdoc/>
    public override DbDataSource CreateDataSource(string connectionString)
        => GaussDBDataSource.Create(connectionString);

    #region IServiceProvider Members

    /// <summary>
    /// Gets the service object of the specified type.
    /// </summary>
    /// <param name="serviceType">An object that specifies the type of service object to get.</param>
    /// <returns>A service object of type serviceType, or null if there is no service object of type serviceType.</returns>
    public object? GetService(Type serviceType) => null;

    #endregion
}
