using System.Collections.Concurrent;

namespace HuaweiCloud.GaussDB;

static class GaussDBGlobalClusterStatusTracker
{
    // 记录同一组 URL 最近一次成功命中的主簇，用于下次优先尝试该 AZ。
    static readonly ConcurrentDictionary<string, string> PreferredClusterKeys = new();

    internal static string? GetPreferredClusterKey(string urlKey)
        => PreferredClusterKeys.TryGetValue(urlKey, out var clusterKey)
            ? clusterKey
            : null;

    internal static void ReportPrimary(string urlKey, string clusterKey)
        => PreferredClusterKeys[urlKey] = clusterKey;

    internal static void Reset()
        => PreferredClusterKeys.Clear();
}
