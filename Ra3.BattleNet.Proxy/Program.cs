using Microsoft.Extensions.Logging;
using NLog.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

var logger = LoggerFactory.Create(builder => builder.AddNLog()).CreateLogger("Proxy");
string serverAddress = "localhost";
_ = RunTcpProxy(18840); // EA FESL login
_ = RunTcpProxy(16667); // Peerchat
_ = RunUdpUploadProxy(27900); // HeartbeatMaster
_ = RunTcpProxy(28910); // QueryMaster
_ = RunTcpProxy(29900); // GPCM
_ = RunTcpProxy(10186); // Balancer
_ = RunTcpProxy(28942); // Replay

async Task RunTcpProxy(int port, int? translatedPort = default)
{
    var tcpListener = new TcpListener(IPAddress.Any, port);
    tcpListener.Start();
    while (true)
    {
        try
        {
            var client = await tcpListener.AcceptTcpClientAsync();
            _ = HandleConnection(client, translatedPort ?? port);
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

async Task RunUdpUploadProxy(int port, int? translatedPort = default)
{
    var @lock = new object();
    var udpClient = new UdpClient(port);
    var responses = new Dictionary<IPEndPoint, Memory<byte>>();
    Action? cancelReceive = null;
    void SetResponse(IPEndPoint remoteEndPoint, Memory<byte> response)
    {
        lock (@lock)
        {
            responses[remoteEndPoint] = response;
            cancelReceive?.Invoke();
        }
    }
    async Task SendResponses()
    {
        Dictionary<IPEndPoint, Memory<byte>> currentResponses;
        lock (@lock)
        {
            if (responses.Count == 0)
            {
                return;
            }
            currentResponses = responses;
            responses = new();
        }
        foreach (var (endPoint, response) in currentResponses)
        {
            try
            {
                await udpClient.SendAsync(response.ToArray(), response.Length, endPoint);
            }
            catch (Exception e)
            {
                logger.LogError(e, "Error sending response to {endPoint}", endPoint);
            }
        }
    }

    while (true)
    {
        try
        {
            using var cancellation = new CancellationTokenSource();
            void CancelThisReceive() => cancellation.Cancel();
            UdpReceiveResult received;
            try
            {
                lock (@lock)
                {
                    cancelReceive += CancelThisReceive;
                    if (responses.Count > 0)
                    {
                        cancelReceive();
                    }
                }
                received = await udpClient.ReceiveAsync(cancellation.Token);
            }
            catch (OperationCanceledException)
            {
                await SendResponses();
                return;
            }
            finally
            {
                lock (@lock)
                {
                    cancelReceive -= CancelThisReceive;
                }
            }
            _ = HandleUdpUpload(received.Buffer, received.RemoteEndPoint, translatedPort ?? port, response =>
            {
                _ = SetResponse(received.RemoteEndPoint, response);
            });
        }
        catch (Exception e)
        {
            logger.LogError(e, "Error accepting connection");
        }
    }
}

async Task HandleUdpUpload(ReadOnlyMemory<byte> buffer, IPEndPoint remoteEndPoint, int port, Action<Memory<byte>> setResponse)
{
    logger.LogInformation("Handling connection from {remoteEndPoint} to {port}", remoteEndPoint, port);
    try
    {
        using var server = new UdpClient();
        await server.SendAsync(buffer);
        // 假如 5 秒之内有回复，就把收到的第一个回复发回去
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var result = await server.ReceiveAsync(timeout.Token);
        setResponse(result.Buffer.AsMemory(0, result.Buffer.Length));
    }
    catch (OperationCanceledException) { }
    catch (Exception e)
    {
        logger.LogError(e, "Error handling connection from {remoteEndPoint} to {port}", remoteEndPoint, port);
    }
}