using System;

namespace HuaweiCloud.GaussDB;

readonly record struct HaEndpoint(string Host, int Port)
{
    internal string Key
        => Host.Contains(':') && !Host.StartsWith("[", StringComparison.Ordinal)
            ? $"[{Host}]:{Port}"
            : $"{Host}:{Port}";

    public override string ToString() => Key;
}
