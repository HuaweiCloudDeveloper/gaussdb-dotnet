using GaussDB.Internal;

namespace GaussDB.BackendMessages;

sealed class ReadyForQueryMessage : IBackendMessage
{
    public BackendMessageCode Code => BackendMessageCode.ReadyForQuery;

    internal TransactionStatus TransactionStatusIndicator { get; private set; }

    internal ReadyForQueryMessage Load(GaussDBReadBuffer buf) {
        TransactionStatusIndicator = (TransactionStatus)buf.ReadByte();
        return this;
    }
}