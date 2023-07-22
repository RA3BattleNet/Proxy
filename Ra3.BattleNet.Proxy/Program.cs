using Microsoft.Extensions.Logging;
using NLog;
using NLog.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

// load configuration
/*
{
  "actualServer": "101.34.40.247",
  "logLevel": "Info",
  "proxies": [
    { "type": "tcp", "port": 18840 }, // EA FESL login 
    { "type": "tcp", "port": 16667 }, // Peerchat
    { "type": "udp", "port": 27900 }, // HeartbeatMaster
    { "type": "tcp", "port": 28910 }, // QueryMaster
    { "type": "tcp", "port": 29900 }, // GPCM
    { "type": "tcp", "port": 10186 }, // Balancer
    { "type": "tcp", "port": 28942 }, // Replay
  ],
  "peerchatPort": 16667
}
*/
Configuration? config;
string logLevel;
Microsoft.Extensions.Logging.ILogger logger;
byte[] peerchatLoginToken = Encoding.UTF8.GetBytes("CRYPT des 1 redalert3pc");
try
{
    var configFile = await File.ReadAllTextAsync("config.json");
    var jsonOptions = new JsonSerializerOptions()
    {
        AllowTrailingCommas = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        ReadCommentHandling = JsonCommentHandling.Skip,
    };
    config = JsonSerializer.Deserialize<Configuration>(configFile, jsonOptions)
        ?? throw new NullReferenceException("Read null configuration value");
    // Use log level defined in config.json, or "Trace" if not defined
    // Use "Off" to disable logging
    logLevel = NLog.LogLevel.FromString(config.LogLevel ?? "Trace").Name;
    LogManager.Configuration.Variables["FileLogLevel"] = logLevel;
    // Display logs on console only when running on Windows and not in a service
    if (OperatingSystem.IsWindows() && Environment.UserInteractive)
    {
        LogManager.Configuration.Variables["ConsoleLogLevel"] = logLevel;
    }
    LogManager.ReconfigExistingLoggers(); // Explicit refresh of Layouts and updates active Logger-objects
    logger = LoggerFactory.Create(builder => builder.AddNLog()).CreateLogger("Proxy");
}
catch (Exception ex)
{
    Console.Error.WriteLine($"Error when initializing: {ex}");
    throw;
}

try
{
    // Get actual server
    var result = await Dns.GetHostAddressesAsync(config.ActualServer);
    IPAddress serverAddress = result.First();
    // Start proxy
    var proxies = config.Proxies.Select(p => p.Type switch
    {
        "tcp" => RunTcpProxy(serverAddress, p.Port, p.ServerPort),
        "udp" => UdpProxy.Run(serverAddress, p.Port, p.ServerPort),
        _ => throw new ArgumentException($"Invalid type for proxy: {p.Type}")
    }).ToArray();
    if (!proxies.Any())
    {
        logger.LogCritical("No proxies defined in config.json");
        return -1;
    }
    await await Task.WhenAny(proxies);
    return 0;
}
catch (Exception ex)
{
    logger.LogCritical(ex, "Proxy server stopped because of unhandled exception");
    throw;
}

async Task RunTcpProxy(IPAddress serverAddress, int port, int? serverPort = default)
{
    var logger = LogManager.GetLogger(serverPort is null ? $"Tcp/{port}" : $"Tcp/{port}/{serverPort}");
    var tcpListener = TcpListener.Create(port);
    tcpListener.Start();
    logger.Info($"{nameof(RunTcpProxy)} started on {{tcpProxy}}", tcpListener.LocalEndpoint);
    while (true)
    {
        try
        {
            var client = await tcpListener.AcceptTcpClientAsync();
            if (port != config.PeerchatPort)
            {
                _ = HandleConnection(logger, serverAddress, client, serverPort ?? port);
            }
            else
            {
                _ = HandlePeerchatConnection(logger, serverAddress, client, serverPort ?? port);
            }
        }
        catch (Exception e)
        {
            logger.Error(e, "Error accepting tcp connection");
        }
    }
}

async Task HandleConnection(Logger logger, IPAddress serverAddress, TcpClient client, int port)
{
    logger.Info("Handling connection from {remoteEndPoint} to {port}", client.Client.RemoteEndPoint, port);
    try
    {
        using var server = new TcpClient();
        await server.ConnectAsync(serverAddress, port);
        using var cancelOnError = new CancellationTokenSource();
        await Task.WhenAll(CopyStream(logger, client.Client, server.Client, cancelOnError),
                           CopyStream(logger, server.Client, client.Client, cancelOnError));
        logger.Info("Connection from {remoteEndPoint} to {port} closed", client.Client.RemoteEndPoint, port);
    }
    catch (Exception e)
    {
        logger.Error(e, "Error handling connection from {remoteEndPoint} to {port}", client.Client.RemoteEndPoint, port);
    }
    finally
    {
        client.Close();
    }
}

async Task HandlePeerchatConnection(Logger logger, IPAddress serverAddress, TcpClient client, int port)
{
    logger.Info("Handling peerchat connection from {remoteEndPoint} to {port}", client.Client.RemoteEndPoint, port);
    try
    {
        using var server = new TcpClient();
        await server.ConnectAsync(serverAddress, port);
        Memory<byte> buffer = new byte[64];
        try
        {
            var bytesReceived = await client.Client.ReceiveAsync(buffer, SocketFlags.None);
            buffer = buffer[..bytesReceived];
            if (!buffer.Span.StartsWith(peerchatLoginToken))
            {
                logger.Error("Invalid peerchat login token received from {remoteEndPoint}, aborting connection", client.Client.RemoteEndPoint);
                return;
            }
            logger.Info("Modify peerchat connection header from {remoteEndPoint} to {port}", client.Client.RemoteEndPoint, port);
            await client.Client.SendAsync(peerchatLoginToken, SocketFlags.None);
            var ipEndPoint = client.Client.RemoteEndPoint as IPEndPoint
                ?? throw new InvalidOperationException("No IPEndPoint in peerchat connection");
            //await client.Client.SendAsync(Encoding.UTF8.GetBytes($" {ipEndPoint.Address}"), SocketFlags.None);
            logger.Info("Send remaining peerchat connection header from {remoteEndPoint} to {port}", client.Client.RemoteEndPoint, port);
            await client.Client.SendAsync(buffer[peerchatLoginToken.Length..], SocketFlags.None);
        }
        catch (Exception e)
        {
            logger.Error(e, "Error handling peerchat connection from {remoteEndPoint} to {port}", client.Client.RemoteEndPoint, port);
        }
        logger.Info("Peerchat connection from {remoteEndPoint} to {port} continue", client.Client.RemoteEndPoint, port);
        using var cancelOnError = new CancellationTokenSource();
        await Task.WhenAll(CopyStream(logger, client.Client, server.Client, cancelOnError),
                           CopyStream(logger, server.Client, client.Client, cancelOnError));
        logger.Info("Peerchat connection from {remoteEndPoint} to {port} closed", client.Client.RemoteEndPoint, port);
    }
    catch (Exception e)
    {
        logger.Error(e, "Error handling connection from {remoteEndPoint} to {port}", client.Client.RemoteEndPoint, port);
    }
    finally
    {
        client.Close();
    }
}

async Task CopyStream(Logger logger, Socket receiveFrom, Socket sendTo, CancellationTokenSource cancelOnError)
{
    try
    {
        var cancellationToken = cancelOnError.Token;
        Memory<byte> buffer = new byte[4096];
        while (true)
        {
            var read = await receiveFrom.ReceiveAsync(buffer, SocketFlags.None, cancellationToken);
            logger.Debug("Received {bytes} bytes from {remote}", read, receiveFrom.RemoteEndPoint);
            if (read == 0)
            {
                sendTo.Shutdown(SocketShutdown.Send);
                break;
            }
            await sendTo.SendAsync(buffer[..read], SocketFlags.None, cancellationToken);
        }
    }
    catch when (!cancelOnError.IsCancellationRequested)
    {
        cancelOnError.Cancel();
        throw;
    }
}

record Configuration(string ActualServer, ProxyConfiguration[] Proxies, int PeerchatPort, string? LogLevel = default);

record ProxyConfiguration(string Type, int Port, int? ServerPort = default);

class UdpProxy
{
    private record Connection(Task<Socket> Socket, DateTimeOffset LastUpdateTime, CancellationTokenSource Cancellation)
    {
        public Connection Update() => this with { LastUpdateTime = DateTimeOffset.UtcNow };
    }
    private static readonly IPEndPoint Template = new(IPAddress.IPv6Any, 0);
    private readonly Logger _logger;
    private readonly IPEndPoint _server;
    private readonly Socket _toUser = new(SocketType.Dgram, ProtocolType.Udp);
    private readonly ConcurrentDictionary<EndPoint, Connection> _connections = new();

    public static async Task Run(IPAddress serverAddress, int port, int? serverPort = default, CancellationToken cancellation = default)
    {
        var proxy = new UdpProxy(new(serverAddress, serverPort ?? port), port);
        Memory<byte> data = new byte[2048];
        while (true)
        {
            try
            {
                await proxy.HandleIncomingUserData(data, cancellation);
            }
            catch (Exception e)
            {
                proxy._logger.Error(e, "Error handling connection to {server}", serverAddress);
            }
        }
    }

    public UdpProxy(IPEndPoint server, int toUserPort)
    {
        _logger = LogManager.GetLogger(server.Port == toUserPort ? $"Udp/{toUserPort}" : $"Udp/{toUserPort}/{server.Port}", typeof(UdpProxy));
        _server = server;
        _toUser.Bind(new IPEndPoint(IPAddress.IPv6Any, toUserPort));
        _ = Task.Run(PruneConnections);
        _logger.Info($"{nameof(UdpProxy)} starting on {{udpProxy}}", _toUser.LocalEndPoint);
    }

    public async Task HandleIncomingUserData(Memory<byte> data, CancellationToken cancellation = default)
    {
        var r = await _toUser.ReceiveFromAsync(data, SocketFlags.None, Template, cancellation);
        _logger.Debug("Forwarding {bytes} bytes from {user} to real server", r.ReceivedBytes, r.RemoteEndPoint);
        var c = _connections.AddOrUpdate(r.RemoteEndPoint, NewConnection, (_, c) => c.Update());
        await (await c.Socket).SendAsync(data[..r.ReceivedBytes], SocketFlags.None, cancellation);
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
                    _logger.Debug("Received {bytes} bytes from real server for {user}", receivedSize, user);
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
                    _logger.Error(ex, "Error handling connection from {user}", user);
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
            if (toBeRemoved.Length > 0)
            {
                _logger.Info("Removing {removingCount} dead udp connections", toBeRemoved.Length);
            }
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
                        _logger.Error(ex, "Error removing connection to {user}", key);
                    }
                }
            }
            await Task.Delay(TimeSpan.FromMinutes(1));
        }
    }
}
