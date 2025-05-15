using HuaweiCloud.GaussDB.Internal;

namespace HuaweiCloud.GaussDB.BackendMessages;

sealed class BackendKeyDataMessage : IBackendMessage
{
    public BackendMessageCode Code => BackendMessageCode.BackendKeyData;

    internal int BackendProcessId { get; }
    internal int BackendSecretKey { get; }

    internal BackendKeyDataMessage(GaussDBReadBuffer buf)
    {
        BackendProcessId = buf.ReadInt32();
        BackendSecretKey = buf.ReadInt32();
    }
}
