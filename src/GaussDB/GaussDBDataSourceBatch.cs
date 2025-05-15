using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using GaussDB.Properties;

namespace GaussDB;

sealed class GaussDBDataSourceBatch : GaussDBBatch
{
    internal GaussDBDataSourceBatch(GaussDBConnection connection)
        : base(static (conn, batch) => new GaussDBDataSourceCommand(batch, DefaultBatchCommandsSize, conn), connection)
    {
    }

    // The below are incompatible with batches executed directly against DbDataSource, since no DbConnection
    // is involved at the user API level and the batch owns the DbConnection.
    public override void Prepare()
        => throw new NotSupportedException(GaussDBStrings.NotSupportedOnDataSourceBatch);

    public override Task PrepareAsync(CancellationToken cancellationToken = default)
        => throw new NotSupportedException(GaussDBStrings.NotSupportedOnDataSourceBatch);

    protected override DbConnection? DbConnection
    {
        get => throw new NotSupportedException(GaussDBStrings.NotSupportedOnDataSourceBatch);
        set => throw new NotSupportedException(GaussDBStrings.NotSupportedOnDataSourceBatch);
    }

    protected override DbTransaction? DbTransaction
    {
        get => throw new NotSupportedException(GaussDBStrings.NotSupportedOnDataSourceBatch);
        set => throw new NotSupportedException(GaussDBStrings.NotSupportedOnDataSourceBatch);
    }
}
