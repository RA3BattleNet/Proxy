using Microsoft.Extensions.Logging;
using NLog;
using NLog.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

var logger = LoggerFactory.Create(builder => builder.AddNLog()).CreateLogger("Proxy");
string serverAddress = "localhost";
await Task.WhenAny(new[]
{
    RunTcpProxy(18840), // EA FESL login
    RunTcpProxy(16667), // Peerchat
    RunUdpProxy(27900), // HeartbeatMaster
    RunTcpProxy(28910), // QueryMaster
    RunTcpProxy(29900), // GPCM
    RunTcpProxy(10186), // Balancer
    RunTcpProxy(28942), // Replay
});

async Task RunTcpProxy(int port, int? serverPort = default)
{
    var tcpListener = TcpListener.Create(port);
    tcpListener.Start();
    while (true)
    {
        try
        {
            var client = await tcpListener.AcceptTcpClientAsync();
            _ = HandleConnection(client, serverPort ?? port);
        }
        catch (Exception e)
        {
            logger.LogError(e, "Error accepting connection");
        }
    }
}

async Task HandleConnection(TcpClient client, int port)
{
    logger.LogInformation("Handling connection from {remoteEndPoint} to {port}", client.Client.RemoteEndPoint, port);
    try
    {
        using var server = new TcpClient();
        await server.ConnectAsync(serverAddress, port);
        await Task.WhenAll(CopyStream(client.GetStream(), server.GetStream()), CopyStream(server.GetStream(), client.GetStream()));
    }
    catch (Exception e)
    {
        logger.LogError(e, "Error handling connection from {remoteEndPoint} to {port}", client.Client.RemoteEndPoint, port);
    }
    finally
    {
        client.Close();
    }
}

async Task CopyStream(NetworkStream receiveFrom, NetworkStream sendTo)
{
    var buffer = new byte[4096];
    while (true)
    {
        var read = await receiveFrom.ReadAsync(buffer);
        if (read == 0)
        {
            break;
        }
        await sendTo.WriteAsync(buffer.AsMemory(0, read));
    }
}

async Task RunUdpProxy(int port, int? serverPort = default)
{
    var proxy = new UdpProxy(new(IPAddress.Parse(serverAddress), serverPort ?? port), port);
    Memory<byte> data = new byte[2048];
    while (true)
    {
        try
        {
            await proxy.HandleIncomingUserData(data);
        }
        catch (Exception e)
        {
            logger.LogError(e, "Error handling connection to {port}", port);
        }
    }
}

class UdpProxy
{
    private record Connection(Task<Socket> Socket, DateTimeOffset LastUpdateTime, CancellationTokenSource Cancellation)
    {
        public Connection Update() => this with { LastUpdateTime = DateTimeOffset.UtcNow };
    }
    private static readonly Logger Logger = LogManager.GetCurrentClassLogger();
    private static readonly IPEndPoint Template = new(IPAddress.IPv6Any, 0);
    private readonly IPEndPoint _server;
    private readonly Socket _toUser = new(SocketType.Dgram, ProtocolType.Udp);
    private readonly ConcurrentDictionary<EndPoint, Connection> _connections = new();

    public UdpProxy(IPEndPoint server, int toUserPort)
    {
        _server = server;
        _toUser.Bind(new IPEndPoint(IPAddress.IPv6Any, toUserPort));
        _ = Task.Run(PruneConnections);
    }

    public async Task HandleIncomingUserData(Memory<byte> data, CancellationToken cancel = default)
    {
        var r = await _toUser.ReceiveFromAsync(data, SocketFlags.None, Template, cancel);
        Logger.Info("Forwarding {bytes} bytes from {user} to real server", r.ReceivedBytes, r.RemoteEndPoint);
        var c = _connections.AddOrUpdate(r.RemoteEndPoint, NewConnection, (_, c) => c.Update());
        await (await c.Socket).SendAsync(data[..r.ReceivedBytes], SocketFlags.None, cancel);
    }

    private Connection NewConnection(EndPoint user)
    {
        static async Task<Socket> GetSocket(IPEndPoint server, CancellationToken cancellationToken)
        {
            var socket = new Socket(SocketType.Dgram, ProtocolType.Udp);
            await socket.ConnectAsync(server, cancellationToken);
            return socket;
        }
        var cancellation = new CancellationTokenSource();
        var cancellationToken = cancellation.Token;
        var futureSocket = GetSocket(_server, cancellationToken);
        _ = Task.Run(async () =>
        {
            Memory<byte> buffer = new byte[2048];
            try
            {
                var socket = await futureSocket;
                while (true)
                {
                    var receivedSize = await socket.ReceiveAsync(buffer, SocketFlags.None, cancellationToken);
                    Logger.Info("Received {bytes} bytes from real server for {user}", receivedSize, user);
                    _ = _connections.AddOrUpdate(user,
                        _ => throw new InvalidOperationException($"AddOrUpdate called for dead connection to {user}"),
                        (_, c) => c.Update());
                    await _toUser.SendToAsync(buffer[..receivedSize], SocketFlags.None, user, cancellationToken);
                }
            }
            catch (Exception ex)
            {
                if (ex is not OperationCanceledException)
                {
                    Logger.Error(ex, "Error handling connection from {user}", user);
                }
            }
        }, cancellationToken);
        return new(futureSocket, DateTimeOffset.UtcNow, cancellation);
    }

    private async Task PruneConnections()
    {
        while (true)
        {
            var now = DateTimeOffset.UtcNow;
            var toBeRemoved = _connections
                .Where(kv => now - kv.Value.LastUpdateTime > TimeSpan.FromMinutes(1))
                .Select(kv => kv.Key)
                .ToArray();
            foreach (var key in toBeRemoved)
            {
                if (_connections.TryRemove(key, out var connection))
                {
                    try
                    {
                        connection.Cancellation.Cancel();
                    }
                    catch (Exception ex)
                    {
                        Logger.Error(ex, "Error removing connection to {user}", key);
                    }
                }
            }
            await Task.Delay(TimeSpan.FromMinutes(1));
        }
    }
}
