using System;

namespace HuaweiCloud.GaussDB;

readonly record struct HaEndpoint(string Host, int Port)
{
    // Normalize host:port keys so endpoint dedupe stays stable across IPv4/IPv6 inputs.
    internal string Key
        => Host.Contains(':') && !Host.StartsWith("[", StringComparison.Ordinal)
            ? $"[{Host}]:{Port}"
            : $"{Host}:{Port}";

    public override string ToString() => Key;
}
