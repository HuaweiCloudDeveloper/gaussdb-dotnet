using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace HuaweiCloud.GaussDB;

static class GaussDBCoordinatorListTracker
{
    sealed class Entry
    {
        // 同一个簇的 CN 刷新要做单飞控制，避免并发建连时同时打爆 pgxc_node。
        internal readonly SemaphoreSlim Semaphore = new(1, 1);
        internal volatile HaEndpoint[]? Snapshot;
        internal long LastAttemptTicks;
    }

    static readonly ConcurrentDictionary<string, Entry> Entries = new();

    internal static ValueTask<HaEndpoint[]?> GetSnapshotAsync(
        string clusterKey,
        TimeSpan refreshInterval,
        Func<CancellationToken, ValueTask<HaEndpoint[]?>> refreshFactory,
        bool async,
        CancellationToken cancellationToken)
    {
        var entry = Entries.GetOrAdd(clusterKey, static _ => new Entry());
        if (TryGetFreshSnapshot(entry, refreshInterval, out var snapshot))
            return new(snapshot);

        return async
            ? RefreshAsync(entry, refreshInterval, refreshFactory, cancellationToken)
            : new(RefreshAsync(entry, refreshInterval, refreshFactory, cancellationToken).GetAwaiter().GetResult());
    }

    internal static void SeedSnapshotForTesting(string clusterKey, params HaEndpoint[] endpoints)
    {
        var entry = Entries.GetOrAdd(clusterKey, static _ => new Entry());
        entry.Snapshot = endpoints.Length == 0 ? null : endpoints;
        entry.LastAttemptTicks = DateTime.UtcNow.Ticks;
    }

    internal static void Reset()
        => Entries.Clear();

    static bool TryGetFreshSnapshot(Entry entry, TimeSpan refreshInterval, out HaEndpoint[]? snapshot)
    {
        snapshot = entry.Snapshot;
        var lastAttemptTicks = entry.LastAttemptTicks;
        if (lastAttemptTicks == 0)
            return false;

        var lastAttemptUtc = new DateTime(lastAttemptTicks, DateTimeKind.Utc);
        return lastAttemptUtc >= DateTime.UtcNow - refreshInterval;
    }

    static async ValueTask<HaEndpoint[]?> RefreshAsync(
        Entry entry,
        TimeSpan refreshInterval,
        Func<CancellationToken, ValueTask<HaEndpoint[]?>> refreshFactory,
        CancellationToken cancellationToken)
    {
        await entry.Semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (TryGetFreshSnapshot(entry, refreshInterval, out var snapshot))
                return snapshot;

            entry.LastAttemptTicks = DateTime.UtcNow.Ticks;

            try
            {
                snapshot = await refreshFactory(cancellationToken).ConfigureAwait(false);
                entry.Snapshot = snapshot is { Length: > 0 } ? snapshot : null;
            }
            catch
            {
                // 刷新失败时清空快照，让后续路由回退到 seed hosts，而不是继续信任陈旧列表。
                entry.Snapshot = null;
            }

            return entry.Snapshot;
        }
        finally
        {
            entry.Semaphore.Release();
        }
    }
}
