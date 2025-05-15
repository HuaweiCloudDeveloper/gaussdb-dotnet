using System;
using System.Threading.Tasks;

namespace GaussDB;

interface ICancelable : IDisposable, IAsyncDisposable
{
    void Cancel();

    Task CancelAsync();
}