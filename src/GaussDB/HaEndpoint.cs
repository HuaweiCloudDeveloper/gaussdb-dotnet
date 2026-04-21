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

readonly record struct HaCoordinatorNode(string NodeName, HaEndpoint HostEndpoint, HaEndpoint EipEndpoint)
{
    internal HaEndpoint GetPreferredEndpoint(bool usingEip)
        => usingEip ? EipEndpoint : HostEndpoint;

    internal bool Matches(HaEndpoint endpoint)
        => HostEndpoint.Key == endpoint.Key || EipEndpoint.Key == endpoint.Key;
}
