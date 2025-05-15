using HuaweiCloud.GaussDB.Internal;

namespace HuaweiCloud.GaussDB.BackendMessages;

sealed class ReadyForQueryMessage : IBackendMessage
{
    public BackendMessageCode Code => BackendMessageCode.ReadyForQuery;

    internal TransactionStatus TransactionStatusIndicator { get; private set; }

    internal ReadyForQueryMessage Load(GaussDBReadBuffer buf) {
        TransactionStatusIndicator = (TransactionStatus)buf.ReadByte();
        return this;
    }
}
