using GaussDB.Internal;
using GaussDB.Util;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;

namespace GaussDB;

sealed class MultiHostDataSourceWrapper(GaussDBMultiHostDataSource wrappedSource, TargetSessionAttributes targetSessionAttributes)
    : GaussDBDataSource(CloneSettingsForTargetSessionAttributes(wrappedSource.Settings, targetSessionAttributes), wrappedSource.Configuration)
{
    internal override bool OwnsConnectors => false;

    public override void Clear() => wrappedSource.Clear();

    static GaussDBConnectionStringBuilder CloneSettingsForTargetSessionAttributes(
        GaussDBConnectionStringBuilder settings,
        TargetSessionAttributes targetSessionAttributes)
    {
        var clonedSettings = settings.Clone();
        clonedSettings.TargetSessionAttributesParsed = targetSessionAttributes;
        return clonedSettings;
    }

    internal override (int Total, int Idle, int Busy) Statistics => wrappedSource.Statistics;

    internal override ValueTask<GaussDBConnector> Get(GaussDBConnection conn, GaussDBTimeout timeout, bool async, CancellationToken cancellationToken)
        => wrappedSource.Get(conn, timeout, async, cancellationToken);
    internal override bool TryGetIdleConnector([NotNullWhen(true)] out GaussDBConnector? connector)
        => throw new GaussDBException("GaussDB bug: trying to get an idle connector from " + nameof(MultiHostDataSourceWrapper));
    internal override ValueTask<GaussDBConnector?> OpenNewConnector(GaussDBConnection conn, GaussDBTimeout timeout, bool async, CancellationToken cancellationToken)
        => throw new GaussDBException("GaussDB bug: trying to open a new connector from " + nameof(MultiHostDataSourceWrapper));
    internal override void Return(GaussDBConnector connector)
        => wrappedSource.Return(connector);

    internal override void AddPendingEnlistedConnector(GaussDBConnector connector, Transaction transaction)
        => wrappedSource.AddPendingEnlistedConnector(connector, transaction);
    internal override bool TryRemovePendingEnlistedConnector(GaussDBConnector connector, Transaction transaction)
        => wrappedSource.TryRemovePendingEnlistedConnector(connector, transaction);
    internal override bool TryRentEnlistedPending(Transaction transaction, GaussDBConnection connection,
        [NotNullWhen(true)] out GaussDBConnector? connector)
        => wrappedSource.TryRentEnlistedPending(transaction, connection, out connector);
}
