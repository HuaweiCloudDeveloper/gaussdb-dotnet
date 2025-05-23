using System;
using System.Diagnostics;
using System.Threading;
using static System.Threading.Timeout;

namespace HuaweiCloud.GaussDB.Util;

/// <summary>
/// A wrapper around <see cref="CancellationTokenSource"/> to simplify reset management.
/// </summary>
/// <remarks>
/// Since there's no way to reset a <see cref="CancellationTokenSource"/> once it was cancelled,
/// we need to make sure that an existing cancellation token source hasn't been cancelled,
/// every time we start it (see https://github.com/dotnet/runtime/issues/4694).
/// </remarks>
sealed class ResettableCancellationTokenSource(TimeSpan timeout) : IDisposable
{
    bool isDisposed;

    public TimeSpan Timeout { get; set; } = timeout;

    CancellationTokenSource _cts = new();
    CancellationTokenRegistration? _registration;

    /// <summary>
    /// Used, so we wouldn't concurrently use the cts for the cancellation, while it's being disposed
    /// </summary>
    readonly object lockObject = new();

#if DEBUG
    bool _isRunning;
#endif

    public ResettableCancellationTokenSource() : this(InfiniteTimeSpan)
    {
    }

    /// <summary>
    /// Set the timeout on the wrapped <see cref="CancellationTokenSource"/>
    /// and make sure that it hasn't been cancelled yet
    /// </summary>
    /// <param name="cancellationToken">
    /// An optional token to cancel the asynchronous operation. The default value is <see cref="CancellationToken.None"/>.
    /// </param>
    /// <returns>The <see cref="CancellationToken"/> from the wrapped <see cref="CancellationTokenSource"/></returns>
    public CancellationToken Start(CancellationToken cancellationToken = default)
    {
#if DEBUG
        Debug.Assert(!_isRunning);
#endif
        lock (lockObject)
        {
            // if there was an attempt to cancel while the connector was breaking
            // we do nothing and return the default token
            // as we're going to fail while reading or writing anyway
            if (isDisposed)
            {
#if DEBUG
                _isRunning = true;
#endif
                return CancellationToken.None;
            }

            _cts.CancelAfter(Timeout);
            if (_cts.IsCancellationRequested)
            {
                _cts.Dispose();
                _cts = new CancellationTokenSource(Timeout);
            }
        }
        if (cancellationToken.CanBeCanceled)
            _registration = cancellationToken.Register(cts => ((CancellationTokenSource)cts!).Cancel(), _cts);
#if DEBUG
        _isRunning = true;
#endif
        return _cts.Token;
    }

    /// <summary>
    /// Restart the timeout on the wrapped <see cref="CancellationTokenSource"/> without reinitializing it,
    /// even if <see cref="IsCancellationRequested"/> is already set to <see langword="true"/>
    /// </summary>
    public void RestartTimeoutWithoutReset()
    {
        lock (lockObject)
        {
            // if there was an attempt to cancel while the connector was breaking
            // we do nothing and return the default token
            // as we're going to fail while reading or writing anyway
            if (!isDisposed)
                _cts.CancelAfter(Timeout);
        }
    }

    /// <summary>
    /// Reset the wrapper to contain a unstarted and uncancelled <see cref="CancellationTokenSource"/>
    /// in order make sure the next call to <see cref="Start"/> will not invalidate
    /// the cancellation token.
    /// </summary>
    /// <returns>The <see cref="CancellationToken"/> from the wrapped <see cref="CancellationTokenSource"/></returns>
    public CancellationToken Reset()
    {
        _registration?.Dispose();
        _registration = null;
        lock (lockObject)
        {
            // if there was an attempt to cancel while the connector was breaking
            // we do nothing and return
            // as we're going to fail while reading or writing anyway
            if (isDisposed)
            {
#if DEBUG
                _isRunning = false;
#endif
                return CancellationToken.None;
            }

            _cts.CancelAfter(InfiniteTimeSpan);
            if (_cts.IsCancellationRequested)
            {
                _cts.Dispose();
                _cts = new CancellationTokenSource();
            }
        }
#if DEBUG
        _isRunning = false;
#endif
        return _cts.Token;
    }

    /// <summary>
    /// Reset the wrapper to contain a unstarted and uncancelled <see cref="CancellationTokenSource"/>
    /// in order make sure the next call to <see cref="Start"/> will not invalidate
    /// the cancellation token.
    /// </summary>
    public void ResetCts()
    {
        if (_cts.IsCancellationRequested)
        {
            _cts.Dispose();
            _cts = new CancellationTokenSource();
        }
    }

    /// <summary>
    /// Set the timeout on the wrapped <see cref="CancellationTokenSource"/>
    /// to <see cref="System.Threading.Timeout.InfiniteTimeSpan"/>
    /// </summary>
    /// <remarks>
    /// <see cref="IsCancellationRequested"/> can still arrive at a state
    /// where it's value is <see langword="true"/> if the <see cref="CancellationToken"/>
    /// passed to <see cref="Start"/> gets a cancellation request.
    /// If this is the case it will be resolved upon the next call to <see cref="Start"/>
    /// or <see cref="Reset"/>. Calling <see cref="Stop"/> multiple times or without calling
    /// <see cref="Start"/> first will do no any harm (besides eating a tiny amount of CPU cycles).
    /// </remarks>
    public void Stop()
    {
        _registration?.Dispose();
        _registration = null;
        lock (lockObject)
        {
            // if there was an attempt to cancel while the connector was breaking
            // we do nothing
            if (!isDisposed)
                _cts.CancelAfter(InfiniteTimeSpan);
        }
#if DEBUG
        _isRunning = false;
#endif
    }

    /// <summary>
    /// Cancel the wrapped <see cref="CancellationTokenSource"/>
    /// </summary>
    public void Cancel()
    {
        lock (lockObject)
        {
            // if there was an attempt to cancel while the connector was breaking
            // we do nothing
            if (isDisposed)
                return;
            _cts.Cancel();
        }
    }

    /// <summary>
    /// Cancel the wrapped <see cref="CancellationTokenSource"/> after delay
    /// </summary>
    public void CancelAfter(int delay)
    {
        lock (lockObject)
        {
            // if there was an attempt to cancel while the connector was breaking
            // we do nothing
            if (isDisposed)
                return;
            _cts.CancelAfter(delay);
        }
    }

    /// <summary>
    /// The <see cref="CancellationToken"/> from the wrapped
    /// <see cref="CancellationTokenSource"/> .
    /// </summary>
    /// <remarks>
    /// The token is only valid after calling <see cref="Start"/>
    /// and before calling <see cref="Start"/> the next time.
    /// Otherwise you may end up with a token that has already been
    /// cancelled or belongs to a cancellation token source that has
    /// been disposed.
    /// </remarks>
    public CancellationToken Token => _cts.Token;

    public bool IsCancellationRequested => _cts.IsCancellationRequested;

    public void Dispose()
    {
        Debug.Assert(!isDisposed);

        lock (lockObject)
        {
            _registration?.Dispose();
            _cts.Dispose();

            isDisposed = true;
        }
    }
}
