using System;
using System.IO;
using System.Linq;
using BenchmarkDotNet.Attributes;

namespace HuaweiCloud.GaussDB.Benchmarks;

public class UnixDomainSocket
{
    readonly GaussDBConnection _tcpipConn;
    readonly GaussDBCommand _tcpipCmd;
    readonly GaussDBConnection _unixConn;
    readonly GaussDBCommand _unixCmd;

    public UnixDomainSocket()
    {
        _tcpipConn = BenchmarkEnvironment.OpenConnection();
        _tcpipCmd = new GaussDBCommand("SELECT @p", _tcpipConn);
        _tcpipCmd.Parameters.AddWithValue("p", new string('x', 10000));

        var port = new GaussDBConnectionStringBuilder(BenchmarkEnvironment.ConnectionString).Port;
        var candidateDirectories = new[] { "/var/run/postgresql", "/tmp" };
        var dir = candidateDirectories.FirstOrDefault(d => File.Exists(Path.Combine(d, $".s.PGSQL.{port}")));
        if (dir == null)
            throw new Exception("No PostgreSQL unix domain socket was found");

        var connString = new GaussDBConnectionStringBuilder(BenchmarkEnvironment.ConnectionString)
        {
            Host = dir
        }.ToString();
        _unixConn = new GaussDBConnection(connString);
        _unixConn.Open();
        _unixCmd = new GaussDBCommand("SELECT @p", _unixConn);
        _unixCmd.Parameters.AddWithValue("p", new string('x', 10000));
    }

    [Benchmark(Baseline = true)]
    public string Tcpip() => (string)_tcpipCmd.ExecuteScalar()!;

    [Benchmark]
    public string UnixDomain() => (string)_unixCmd.ExecuteScalar()!;
}
