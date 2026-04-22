using System;

namespace HuaweiCloud.GaussDB;

readonly record struct HaEndpoint(string Host, int Port)
{
    // 统一 host:port 的字符串形式，兼容 IPv6，便于做 endpoint 去重和字典索引。
    internal string Key
        => Host.Contains(':') && !Host.StartsWith("[", StringComparison.Ordinal)
            ? $"[{Host}]:{Port}"
            : $"{Host}:{Port}";

    public override string ToString() => Key;
}

readonly record struct HaCoordinatorNode(string NodeName, HaEndpoint HostEndpoint, HaEndpoint EipEndpoint)
{
    // 动态快照里同时带内网地址和 EIP 地址，具体使用哪一个由 UsingEip 决定。
    internal HaEndpoint GetPreferredEndpoint(bool usingEip)
        => usingEip ? EipEndpoint : HostEndpoint;

    // seed 绑定时，只要命中 host/eip 之一，就认为是同一个逻辑 CN。
    internal bool Matches(HaEndpoint endpoint)
        => HostEndpoint.Key == endpoint.Key || EipEndpoint.Key == endpoint.Key;
}
