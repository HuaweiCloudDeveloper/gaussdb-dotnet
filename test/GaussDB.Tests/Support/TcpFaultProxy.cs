using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace HuaweiCloud.GaussDB.Tests.Support;

sealed class TcpFaultProxy : IAsyncDisposable
{
    readonly TcpListener _listener;
    readonly string _targetHost;
    readonly int _targetPort;
    readonly CancellationTokenSource _shutdownCts = new();
    readonly ConcurrentDictionary<int, ConnectionPair> _connections = new();
    readonly Task _acceptLoopTask;

    int _nextConnectionId;
    bool _disabled;
    volatile bool _rejectNewConnections;

    internal string Host => IPAddress.Loopback.ToString();
    internal int Port { get; }
    internal string Endpoint => $"{Host}:{Port}";
    internal int ActiveConnectionCount => _connections.Count;

    TcpFaultProxy(string targetHost, int targetPort)
    {
        _targetHost = targetHost;
        _targetPort = targetPort;

        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _acceptLoopTask = RunAcceptLoopAsync();
    }

    internal static TcpFaultProxy Start(string targetHost, int targetPort)
        => new(targetHost, targetPort);

    // 之后新建的连接直接拒绝，但不影响当前已经建立的转发连接。
    internal void RejectNewConnections()
        => _rejectNewConnections = true;

    // 恢复接受新连接，让测试可以验证“故障后恢复”路径。
    internal void AcceptNewConnections()
        => _rejectNewConnections = false;

    internal void DisconnectExistingConnections()
    {
        // 主动断开所有现有连接，用来模拟链路瞬断。
        foreach (var connection in _connections.Values)
            connection.Close();
    }

    internal async Task DisableAsync()
    {
        // 完整停掉代理：拒绝新连接、断开旧连接，并退出 accept loop。
        if (_disabled)
            return;

        _disabled = true;
        _rejectNewConnections = true;
        _shutdownCts.Cancel();
        _listener.Stop();

        DisconnectExistingConnections();

        try
        {
            await _acceptLoopTask.ConfigureAwait(false);
        }
        catch
        {
            // Tests only care that the proxy stops forwarding and accepting.
        }
    }

    async Task RunAcceptLoopAsync()
    {
        // 接受本地客户端连接，再为每个连接创建到真实目标的转发对。
        while (!_shutdownCts.IsCancellationRequested)
        {
            TcpClient client;
            try
            {
                client = await _listener.AcceptTcpClientAsync(_shutdownCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (_shutdownCts.IsCancellationRequested)
            {
                break;
            }
            catch (ObjectDisposedException) when (_shutdownCts.IsCancellationRequested || _disabled)
            {
                break;
            }
            catch (SocketException) when (_shutdownCts.IsCancellationRequested || _disabled)
            {
                break;
            }

            if (_rejectNewConnections)
            {
                Abort(client);
                continue;
            }

            _ = HandleClientAsync(client);
        }
    }

    async Task HandleClientAsync(TcpClient client)
    {
        // 每个客户端各自维护一对 client/server socket，互不影响。
        TcpClient? server = null;
        var connectionId = Interlocked.Increment(ref _nextConnectionId);

        try
        {
            server = new TcpClient();
            await server.ConnectAsync(_targetHost, _targetPort, _shutdownCts.Token).ConfigureAwait(false);

            var pair = new ConnectionPair(client, server);
            _connections[connectionId] = pair;
            await pair.RunAsync(_shutdownCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_shutdownCts.IsCancellationRequested)
        {
        }
        catch
        {
            client.Dispose();
            server?.Dispose();
        }
        finally
        {
            _connections.TryRemove(connectionId, out _);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await DisableAsync().ConfigureAwait(false);
        _shutdownCts.Dispose();
    }

    static void Abort(TcpClient client)
    {
        // 尽量用 RST 快速终止连接，避免测试等待正常四次挥手。
        try
        {
            client.Client.LingerState = new LingerOption(true, 0);
        }
        catch
        {
        }

        try
        {
            client.Close();
        }
        catch
        {
        }
    }

    sealed class ConnectionPair(TcpClient client, TcpClient server)
    {
        readonly TcpClient _client = client;
        readonly TcpClient _server = server;

        // 两个方向并行泵流，任一方向结束就整体关闭这对连接。
        internal async Task RunAsync(CancellationToken cancellationToken)
        {
            using (_client)
            using (_server)
            {
                var clientStream = _client.GetStream();
                var serverStream = _server.GetStream();

                var clientToServer = PumpAsync(clientStream, serverStream, cancellationToken);
                var serverToClient = PumpAsync(serverStream, clientStream, cancellationToken);

                await Task.WhenAny(clientToServer, serverToClient).ConfigureAwait(false);
                Close();

                try
                {
                    await Task.WhenAll(clientToServer, serverToClient).ConfigureAwait(false);
                }
                catch
                {
                    // One side is expected to fail once the proxy injects a disconnect.
                }
            }
        }

        internal void Close()
        {
            // 统一关闭 client/server 两端，确保双方都观察到链路中断。
            Abort(_client);
            Abort(_server);
        }

        static async Task PumpAsync(NetworkStream source, NetworkStream destination, CancellationToken cancellationToken)
        {
            await source.CopyToAsync(destination, 81920, cancellationToken).ConfigureAwait(false);
            await destination.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
