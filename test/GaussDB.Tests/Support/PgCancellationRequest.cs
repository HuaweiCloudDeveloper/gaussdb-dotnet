using System.IO;
using HuaweiCloud.GaussDB.Internal;

namespace HuaweiCloud.GaussDB.Tests.Support;

class PgCancellationRequest(GaussDBReadBuffer readBuffer, GaussDBWriteBuffer writeBuffer, Stream stream, int processId, int secret)
{
    public int ProcessId { get; } = processId;
    public int Secret { get; } = secret;

    bool completed;

    public void Complete()
    {
        if (completed)
            return;

        readBuffer.Dispose();
        writeBuffer.Dispose();
        stream.Dispose();

        completed = true;
    }
}
