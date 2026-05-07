using System;
using System.Collections.Concurrent;
using System.Linq;

namespace HuaweiCloud.GaussDB;

static class GaussDBGlobalClusterStatusTracker
{
    const int MaxPreferredClusterKeys = 1024;
    const int TrimPreferredClusterKeysTo = 768;
    static readonly object TrimLock = new();

    sealed class PreferredClusterEntry
    {
        internal PreferredClusterEntry(string clusterKey, long lastAccessTicks)
        {
            ClusterKey = clusterKey;
            LastAccessTicks = lastAccessTicks;
        }

        internal string ClusterKey;
        internal long LastAccessTicks;
    }

    // Records the most recently confirmed primary cluster for the same URL key.
    static readonly ConcurrentDictionary<string, PreferredClusterEntry> PreferredClusterKeys = new();

    internal static string? GetPreferredClusterKey(string urlKey)
    {
        if (!PreferredClusterKeys.TryGetValue(urlKey, out var entry))
            return null;

        entry.LastAccessTicks = DateTime.UtcNow.Ticks;
        return entry.ClusterKey;
    }

    internal static void ReportPrimary(string urlKey, string clusterKey)
    {
        PreferredClusterKeys[urlKey] = new(clusterKey, DateTime.UtcNow.Ticks);
        TrimIfNeeded();
    }

    internal static void Reset()
        => PreferredClusterKeys.Clear();

    static void TrimIfNeeded()
    {
        if (PreferredClusterKeys.Count <= MaxPreferredClusterKeys)
            return;

        lock (TrimLock)
        {
            var currentCount = PreferredClusterKeys.Count;
            if (currentCount <= MaxPreferredClusterKeys)
                return;

            foreach (var key in PreferredClusterKeys
                         .OrderBy(static pair => pair.Value.LastAccessTicks)
                         .Take(currentCount - TrimPreferredClusterKeysTo)
                         .Select(static pair => pair.Key)
                         .ToArray())
                PreferredClusterKeys.TryRemove(key, out _);
        }
    }
}
