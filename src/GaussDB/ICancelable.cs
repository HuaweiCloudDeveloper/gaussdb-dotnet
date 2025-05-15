using System;
using System.Threading.Tasks;

namespace HuaweiCloud.GaussDB;

interface ICancelable : IDisposable, IAsyncDisposable
{
    void Cancel();

    Task CancelAsync();
}
