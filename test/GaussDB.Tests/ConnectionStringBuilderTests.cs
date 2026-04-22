using System;
using NUnit.Framework;

namespace HuaweiCloud.GaussDB.Tests;

class ConnectionStringBuilderTests
{
    [Test]
    public void Basic()
    {
        var builder = new GaussDBConnectionStringBuilder();
        Assert.That(builder.Count, Is.EqualTo(0));
        Assert.That(builder.ContainsKey("server"), Is.True);
        builder.Host = "myhost";
        Assert.That(builder["host"], Is.EqualTo("myhost"));
        Assert.That(builder.Count, Is.EqualTo(1));
        Assert.That(builder.ConnectionString, Is.EqualTo("Host=myhost"));
        builder.Remove("HOST");
        Assert.That(builder["host"], Is.EqualTo(""));
        Assert.That(builder.Count, Is.EqualTo(0));
    }

    [Test]
    public void TryGetValue()
    {
        var builder = new GaussDBConnectionStringBuilder();
        builder.ConnectionString = "Host=myhost";
        Assert.That(builder.TryGetValue("Host", out var value), Is.True);
        Assert.That(value, Is.EqualTo("myhost"));
        Assert.That(builder.TryGetValue("SomethingUnknown", out value), Is.False);
    }

    [Test]
    public void Remove()
    {
        var builder = new GaussDBConnectionStringBuilder();
        builder.SslMode = SslMode.Require;
        Assert.That(builder["SSL Mode"], Is.EqualTo(SslMode.Require));
        builder.Remove("SSL Mode");
        Assert.That(builder.ConnectionString, Is.EqualTo(""));
        builder.CommandTimeout = 120;
        Assert.That(builder["Command Timeout"], Is.EqualTo(120));
        builder.Remove("Command Timeout");
        Assert.That(builder.ConnectionString, Is.EqualTo(""));
    }

    [Test]
    public void Clear()
    {
        var builder = new GaussDBConnectionStringBuilder { Host = "myhost" };
        builder.Clear();
        Assert.That(builder.Count, Is.EqualTo(0));
        Assert.That(builder["host"], Is.EqualTo(""));
        Assert.That(builder.Host, Is.Null);
    }

    [Test]
    public void Removing_resets_to_default()
    {
        var builder = new GaussDBConnectionStringBuilder();
        Assert.That(builder.Port, Is.EqualTo(GaussDBConnection.DefaultPort));
        builder.Port = 8;
        builder.Remove("Port");
        Assert.That(builder.Port, Is.EqualTo(GaussDBConnection.DefaultPort));
    }

    [Test]
    public void Setting_to_null_resets_to_default()
    {
        var builder = new GaussDBConnectionStringBuilder();
        Assert.That(builder.Port, Is.EqualTo(GaussDBConnection.DefaultPort));
        builder.Port = 8;
        builder["Port"] = null;
        Assert.That(builder.Port, Is.EqualTo(GaussDBConnection.DefaultPort));
    }

    [Test]
    public void Enum()
    {
        var builder = new GaussDBConnectionStringBuilder();
        builder.ConnectionString = "SslMode=Require";
        Assert.That(builder.SslMode, Is.EqualTo(SslMode.Require));
        Assert.That(builder.Count, Is.EqualTo(1));
    }

    [Test]
    public void Enum_insensitive()
    {
        var builder = new GaussDBConnectionStringBuilder();
        builder.ConnectionString = "SslMode=require";
        Assert.That(builder.SslMode, Is.EqualTo(SslMode.Require));
        Assert.That(builder.Count, Is.EqualTo(1));
    }

    [Test]
    public void Clone()
    {
        var builder = new GaussDBConnectionStringBuilder();
        builder.Host = "myhost";
        var builder2 = builder.Clone();
        Assert.That(builder2.Host, Is.EqualTo("myhost"));
        Assert.That(builder2["Host"], Is.EqualTo("myhost"));
        Assert.That(builder.Port, Is.EqualTo(GaussDBConnection.DefaultPort));
    }

    [Test]
    public void Conversion_error_throws()
    {
        var builder = new GaussDBConnectionStringBuilder();
        Assert.That(() => builder["Port"] = "hello",
            Throws.Exception.TypeOf<ArgumentException>().With.Message.Contains("Port"));
    }

    [Test]
    public void Invalid_connection_string_throws()
    {
        var builder = new GaussDBConnectionStringBuilder();
        Assert.That(() => builder.ConnectionString = "Server=127.0.0.1;User Id=gaussdb_tests;Pooling:false",
            Throws.Exception.TypeOf<ArgumentException>());
    }

    [Test]
    public void Ha_options_default_values()
    {
        // 验证新增 HA 相关参数的默认值，确保未配置时仍保持兼容行为。
        var builder = new GaussDBConnectionStringBuilder();
        Assert.That(builder.PriorityServers, Is.EqualTo(0));
        Assert.That(builder.AutoBalance, Is.Null);
        Assert.That(builder.RefreshCNIpListTime, Is.EqualTo(0));
        Assert.That(builder.UsingEip, Is.True);
        Assert.That(builder.AutoReconnect, Is.False);
        Assert.That(builder.MaxReconnects, Is.EqualTo(3));
    }

    [Test]
    public void Ha_options_parse_from_connection_string()
    {
        // 验证主备 AZ、动态 CN 刷新和自动重连参数都能从连接串正确解析出来。
        var builder = new GaussDBConnectionStringBuilder(
            "Host=a,b,c,d;priorityServers=2;autoBalance=shufflePriority4;refreshCNIpListTime=30;usingEip=false;autoReconnect=true;maxReconnects=5");

        Assert.That(builder.PriorityServers, Is.EqualTo(2));
        Assert.That(builder.AutoBalance, Is.EqualTo("shufflePriority4"));
        Assert.That(builder.RefreshCNIpListTime, Is.EqualTo(30));
        Assert.That(builder.UsingEip, Is.False);
        Assert.That(builder.AutoReconnect, Is.True);
        Assert.That(builder.MaxReconnects, Is.EqualTo(5));
    }

    [Test]
    public void AutoBalance_invalid_numeric_throws()
        // AutoBalance 只能是受支持的模式名，不能直接填裸数字。
        => Assert.That(
            () => new GaussDBConnectionStringBuilder("Host=a,b;AutoBalance=3"),
            Throws.Exception.TypeOf<ArgumentException>().With.Message.Contains("AutoBalance"));

    [Test]
    public void PriorityServers_invalid_split_throws()
    {
        // PriorityServers 必须真正把 host 列表拆成“优先簇 + 兜底簇”，否则配置无效。
        var builder = new GaussDBConnectionStringBuilder
        {
            Host = "a,b",
            PriorityServers = 2
        };

        Assert.That(
            () => builder.PostProcessAndValidate(),
            Throws.Exception.TypeOf<ArgumentException>().With.Message.Contains("PriorityServers"));
    }

    [Test]
    public void Priority_auto_balance_caps_to_selected_cluster_size()
    {
        // priorityN 在簇内生效时要被当前簇大小截断，避免优先区间超过实际节点数。
        var builder = new GaussDBConnectionStringBuilder
        {
            Host = "a,b,c,d",
            PriorityServers = 2,
            AutoBalance = "priority5"
        };

        builder.PostProcessAndValidate();

        Assert.That(builder.GetEffectivePriorityHostCount(2), Is.EqualTo(2));
        Assert.That(builder.GetEffectivePriorityHostCount(3), Is.EqualTo(3));
    }

    [Test]
    public void RefreshCNIpListTime_zero_is_allowed()
    {
        // 0 表示关闭动态 CN 刷新，应当允许作为显式配置值。
        var builder = new GaussDBConnectionStringBuilder
        {
            RefreshCNIpListTime = 0
        };

        Assert.That(builder.RefreshCNIpListTime, Is.EqualTo(0));
    }

    [Test]
    public void RefreshCNIpListTime_negative_throws()
        // 刷新间隔不能为负数，避免路由刷新逻辑出现无意义配置。
        => Assert.Throws<ArgumentOutOfRangeException>(() =>
            new GaussDBConnectionStringBuilder
            {
                RefreshCNIpListTime = -1
            });

    [Test]
    public void Removing_ha_options_from_clone_allows_single_host_probe_connection_string()
    {
        // seed 反查节点名时会克隆连接串并移除 HA 路由参数，探测连接必须退化成单 host 直连。
        var probeBuilder = new GaussDBConnectionStringBuilder
        {
            Host = "host1,host2,host3",
            PriorityServers = 2,
            AutoBalance = "priority2",
            RefreshCNIpListTime = 30,
            Username = "user",
            Password = "password",
            Database = "postgres"
        }.Clone();

        probeBuilder.Host = "host1";
        probeBuilder.Remove(nameof(GaussDBConnectionStringBuilder.PriorityServers));
        probeBuilder.Remove(nameof(GaussDBConnectionStringBuilder.AutoBalance));
        probeBuilder.Remove(nameof(GaussDBConnectionStringBuilder.RefreshCNIpListTime));

        Assert.DoesNotThrow(() => new GaussDBConnectionStringBuilder(probeBuilder.ConnectionString).PostProcessAndValidate());
    }

    [Test]
    public void MaxReconnects_invalid_throws()
        // 自动重连次数至少为 1，0 会让开启 AutoReconnect 失去意义。
        => Assert.Throws<ArgumentOutOfRangeException>(() =>
            new GaussDBConnectionStringBuilder
            {
                MaxReconnects = 0
            });
}
