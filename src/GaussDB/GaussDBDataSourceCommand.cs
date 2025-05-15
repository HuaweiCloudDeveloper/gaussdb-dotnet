using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using GaussDB.Properties;

namespace GaussDB;

sealed class GaussDBDataSourceCommand : GaussDBCommand
{
    internal GaussDBDataSourceCommand(GaussDBConnection connection)
        : base(cmdText: null, connection)
    {
    }

    // For GaussDBBatch only
    internal GaussDBDataSourceCommand(GaussDBBatch batch, int batchCommandCapacity, GaussDBConnection connection)
        : base(batch, batchCommandCapacity, connection)
    {
    }

    internal override async ValueTask<GaussDBDataReader> ExecuteReader(
        bool async, CommandBehavior behavior,
        CancellationToken cancellationToken)
    {
        await InternalConnection!.Open(async, cancellationToken).ConfigureAwait(false);

        try
        {
            return await base.ExecuteReader(
                    async,
                    behavior | CommandBehavior.CloseConnection,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            try
            {
                await InternalConnection.Close(async).ConfigureAwait(false);
            }
            catch
            {
                // Swallow to allow the original exception to bubble up
            }

            throw;
        }
    }

    // The below are incompatible with commands executed directly against DbDataSource, since no DbConnection
    // is involved at the user API level and the command owns the DbConnection.
    public override void Prepare()
        => throw new NotSupportedException(GaussDBStrings.NotSupportedOnDataSourceCommand);

    public override Task PrepareAsync(CancellationToken cancellationToken = default)
        => throw new NotSupportedException(GaussDBStrings.NotSupportedOnDataSourceCommand);

    protected override DbConnection? DbConnection
    {
        get => throw new NotSupportedException(GaussDBStrings.NotSupportedOnDataSourceCommand);
        set => throw new NotSupportedException(GaussDBStrings.NotSupportedOnDataSourceCommand);
    }

    protected override DbTransaction? DbTransaction
    {
        get => throw new NotSupportedException(GaussDBStrings.NotSupportedOnDataSourceCommand);
        set => throw new NotSupportedException(GaussDBStrings.NotSupportedOnDataSourceCommand);
    }
}
