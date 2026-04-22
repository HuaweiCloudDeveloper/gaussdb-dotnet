using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace HuaweiCloud.GaussDB;

static class GaussDBCoordinatorListTracker
{
    sealed class Entry
    {
        // 同一簇的 CN 刷新要做单飞控制，避免并发建连时同时打爆 pgxc_node。
        internal readonly SemaphoreSlim Semaphore = new(1, 1);
        internal volatile HaCoordinatorNode[]? Snapshot;
        internal long LastAttemptTicks;
    }

    static readonly ConcurrentDictionary<string, Entry> Entries = new();

    // 返回簇级 CN 快照；缓存仍新鲜时直接复用，否则触发一次受节流保护的刷新。
    internal static ValueTask<HaCoordinatorNode[]?> GetSnapshotAsync(
        string clusterKey,
        TimeSpan refreshInterval,
        Func<CancellationToken, ValueTask<HaCoordinatorNode[]?>> refreshFactory,
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
        => SeedSnapshotForTesting(
            clusterKey,
            endpoints.Select(static (endpoint, index) => new HaCoordinatorNode($"seed_{index}", endpoint, endpoint)).ToArray());

    // 测试入口：直接向指定簇注入逻辑 CN 快照，绕过真实 pgxc_node 查询。
    internal static void SeedSnapshotForTesting(string clusterKey, params HaCoordinatorNode[] nodes)
    {
        var entry = Entries.GetOrAdd(clusterKey, static _ => new Entry());
        entry.Snapshot = nodes.Length == 0 ? null : nodes;
        entry.LastAttemptTicks = DateTime.UtcNow.Ticks;
    }

    internal static void Reset()
        => Entries.Clear();

    // 只有“最近一次尝试时间仍处于刷新窗口内”时，当前快照才算新鲜可复用。
    static bool TryGetFreshSnapshot(Entry entry, TimeSpan refreshInterval, out HaCoordinatorNode[]? snapshot)
    {
        snapshot = entry.Snapshot;
        var lastAttemptTicks = entry.LastAttemptTicks;
        if (lastAttemptTicks == 0)
            return false;

        var lastAttemptUtc = new DateTime(lastAttemptTicks, DateTimeKind.Utc);
        return lastAttemptUtc >= DateTime.UtcNow - refreshInterval;
    }

    static async ValueTask<HaCoordinatorNode[]?> RefreshAsync(
        Entry entry,
        TimeSpan refreshInterval,
        Func<CancellationToken, ValueTask<HaCoordinatorNode[]?>> refreshFactory,
        CancellationToken cancellationToken)
    {
        // 通过信号量保证同一簇同一时刻最多只有一个刷新请求真正落到数据库。
        await entry.Semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (TryGetFreshSnapshot(entry, refreshInterval, out var snapshot))
                return snapshot;

            // 不论刷新结果如何，都推进最近尝试时间，对连续失败同样做节流。
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
