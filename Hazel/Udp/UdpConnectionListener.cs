using Microsoft.Extensions.ObjectPool;
using Serilog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Impostor.Hazel.Udp
{
    /// <summary>
    ///     Listens for new UDP connections and creates UdpConnections for them.
    /// </summary>
    /// <inheritdoc />
    public class UdpConnectionListener : NetworkConnectionListener
    {
        private static readonly ILogger Logger = Log.ForContext<UdpConnectionListener>();

        /// <summary>
        /// Whether application-level fragmentation and MTU discovery are enabled for connections created by this listener.
        /// </summary>
        public bool FragmentationEnabled { get; }

        /// <summary>
        /// A callback for early connection rejection. 
        /// * Return false to reject connection.
        /// * A null response is ok, we just won't send anything.
        /// </summary>
        public AcceptConnectionCheck AcceptConnection;

        public delegate bool AcceptConnectionCheck(IPEndPoint endPoint, byte[] input, out byte[] response);

        public bool BlockFurtherEndPoints { get; set; } = false;

        private readonly UdpClient _socket;
        protected readonly ObjectPool<MessageReader> _readerPool;
        private readonly Timer _reliablePacketTimer;
        private readonly ConcurrentDictionary<EndPoint, UdpServerConnection> _allConnections;
        private readonly CancellationTokenSource _stoppingCts;
        private readonly UdpConnectionRateLimit _connectionRateLimit;
        private readonly HashSet<IPAddress> _uniqueIPs = new();
        private readonly ConcurrentDictionary<EndPoint, (int packetCount, bool isBad)> _packetTracking = new();
        private Task _executingTask;
        private long _bytesSent;
        private long _bytesReceived;
        private bool _detectedAttackPort;
        private bool _detectedBadPackets;
        private bool _detectedLeakedConnections;

        /// <summary>
        ///     Creates a new UdpConnectionListener for the given <see cref="IPAddress"/>, port and <see cref="IPMode"/>.
        /// </summary>
        /// <param name="endPoint">The endpoint to listen on.</param>
        public UdpConnectionListener(IPEndPoint endPoint, ObjectPool<MessageReader> readerPool, IPMode ipMode = IPMode.IPv4, bool enableFragmentation = false)
        {
            this.EndPoint = endPoint;
            this.IPMode = ipMode;

            this.FragmentationEnabled = enableFragmentation;

            _readerPool = readerPool;
            _socket = new UdpClient(endPoint);

            _bytesSent = 0;
            _bytesReceived = 0;
            _detectedAttackPort = false;
            _detectedBadPackets = false;
            _detectedLeakedConnections = false;

            if (enableFragmentation)
            {
                try
                {
                    _socket.DontFragment = true;
                }
                catch (SocketException)
                {
                }
            }

            _reliablePacketTimer = new Timer(ManageReliablePackets, null, 100, Timeout.Infinite);

            _allConnections = new ConcurrentDictionary<EndPoint, UdpServerConnection>();

            _stoppingCts = new CancellationTokenSource();
            _stoppingCts.Token.Register(() =>
            {
                _socket.Dispose();
            });

            _connectionRateLimit = new UdpConnectionRateLimit();
        }

        private async void ManageReliablePackets(object state)
        {
            foreach (var kvp in this._allConnections)
            {
                var sock = kvp.Value;
                await sock.ManageReliablePackets();
            }

            try
            {
                this._reliablePacketTimer.Change(100, Timeout.Infinite);
            }
            catch { }
        }

        /// <inheritdoc />
        public override Task StartAsync()
        {
            // Store the task we're executing
            _executingTask = Task.Factory.StartNew(ListenAsync, TaskCreationOptions.LongRunning);

            // If the task is completed then return it, this will bubble cancellation and failure to the caller
            if (_executingTask.IsCompleted)
            {
                return _executingTask;
            }

            // Otherwise it's running
            return Task.CompletedTask;
        }

        private async Task StopAsync()
        {
            // Stop called without start
            if (_executingTask == null)
            {
                return;
            }

            try
            {
                // Signal cancellation to the executing method
                _stoppingCts.Cancel();
            }
            finally
            {
                // Wait until the task completes or the timeout triggers
                await Task.WhenAny(_executingTask, Task.Delay(TimeSpan.FromSeconds(5)));
            }
        }

        /// <summary>
        ///     Instructs the listener to begin listening.
        /// </summary>
        private async Task ListenAsync()
        {
            try
            {
                while (!_stoppingCts.IsCancellationRequested)
                {
                    UdpReceiveResult data;

                    try
                    {
                        data = await _socket.ReceiveAsync();

                        Interlocked.Add(ref _bytesReceived, data.Buffer.Length);

                        if (_uniqueIPs.Add(data.RemoteEndPoint.Address) && _uniqueIPs.Count > 4)
                        {
                            _detectedLeakedConnections = true;
                        }

                        switch (data.RemoteEndPoint.Port)
                        {
                            case 17:
                            case 19:
                            case 53:
                            case 69:
                            case 111:
                            case 123:
                            case 137:
                            case 161:
                            case 162:
                            case 389:
                            case 1194:
                            case 1900:
                            case 3283:
                            case 3389:
                            case 3478:
                            case 3702:
                            case 5683:
                            case 5684:
                            case 11211:
                                if (!_detectedAttackPort)
                                {
                                    Logger.Warning("Port {0} get attacked by {1}", EndPoint.Port, data.RemoteEndPoint.ToString());
                                    _detectedAttackPort = true;
                                }
                                continue;

                            default:
                                if (data.RemoteEndPoint.Port < 1024)
                                {
                                    if (!_detectedAttackPort)
                                    {
                                        Logger.Warning("Port {0} received reserved port packet from {1}", EndPoint.Port, data.RemoteEndPoint.ToString());
                                        _detectedAttackPort = true;
                                    }
                                    continue;
                                }
                                break;
                        }

                        if (data.Buffer.Length == 0)
                        {
                            continue;
                        }
                    }
                    catch (SocketException)
                    {
                        // Client no longer reachable, pretend it didn't happen
                        continue;
                    }
                    catch (ObjectDisposedException)
                    {
                        // Socket was disposed, don't care.
                        return;
                    }

                    await ProcessData(data);
                }
            }
            catch (Exception e)
            {
                Logger.Error(e, "Listen loop error");
            }
        }

        protected virtual async ValueTask ProcessData(UdpReceiveResult data)
        {
            // Get client from active clients
            if (!_allConnections.TryGetValue(data.RemoteEndPoint, out var client))
            {
                // Track packets for the remote endpoint
                var packetInfo = _packetTracking.GetOrAdd(data.RemoteEndPoint, (0, false));

                // Check if the packet is a valid hello packet
                if (data.Buffer[0] != (byte)UdpSendOption.Hello)
                {
                    if (data.Buffer[0] is not (byte)UdpSendOption.Acknowledgement and not (byte)UdpSendOption.Ping and not (byte)UdpSendOption.Disconnect and not 0)
                    {
                        packetInfo.packetCount++;
                    }

                    if (packetInfo.packetCount >= 4)
                    {
                        _detectedBadPackets = true;
                        _packetTracking[data.RemoteEndPoint] = (packetInfo.packetCount, true);
                    }

                    return;
                }
                else
                {
                    // Reset packet count if a valid hello packet is received
                    _packetTracking[data.RemoteEndPoint] = (0, false);
                }

                if (BlockFurtherEndPoints)
                {
                    return;
                }

                // Check rateLimit.
                if (!_connectionRateLimit.IsAllowed(data.RemoteEndPoint.Address))
                {
                    Logger.Warning("Ratelimited connection attempt from {0}.", data.RemoteEndPoint);
                    return;
                }

                // Create new client
                client = new UdpServerConnection(this, data.RemoteEndPoint, IPMode, _readerPool, this.FragmentationEnabled);

                // Store the client
                if (!_allConnections.TryAdd(data.RemoteEndPoint, client))
                {
                    throw new HazelException("Failed to add a connection. This should never happen.");
                }

                // Activate the reader loop of the client
                await client.StartAsync();
            }

            // Write to client.
            await client.Pipeline.Writer.WriteAsync(data.Buffer);
        }

        /// <summary>
        ///     Sends data from the listener socket.
        /// </summary>
        /// <param name="bytes">The bytes to send.</param>
        /// <param name="endPoint">The endpoint to send to.</param>
        internal virtual async ValueTask SendData(byte[] bytes, int length, IPEndPoint endPoint, Action<SocketException> onError = null)
        {
            if (length > bytes.Length) return;

            try
            {
                await _socket.SendAsync(bytes, length, endPoint);

                Interlocked.Add(ref _bytesSent, length);
            }
            catch (SocketException e)
            {
                if (onError != null)
                {
                    onError(e);
                    return;
                }

                Logger.Error(e, "Could not send data as a SocketException occurred");
            }
            catch (ObjectDisposedException)
            {
                //Keep alive timer probably ran, ignore
                return;
            }
        }

        /// <summary>
        ///     Removes a virtual connection from the list.
        /// </summary>
        /// <param name="endPoint">The endpoint of the virtual connection.</param>
        internal void RemoveConnectionTo(EndPoint endPoint)
        {
            this._allConnections.TryRemove(endPoint, out var conn);
        }

        public (long bytesSent, long bytesReceived, bool detectedAttackPort, bool detectedBadPackets, bool detectedLeakedConnections) GetTrafficStatistics()
        {
            return (_bytesSent, _bytesReceived, _detectedAttackPort, _detectedBadPackets, _detectedLeakedConnections);
        }

        /// <inheritdoc />
        public override async ValueTask DisposeAsync()
        {
            foreach (var kvp in _allConnections)
            {
                kvp.Value.Dispose();
            }

            await StopAsync();

            await _reliablePacketTimer.DisposeAsync();

            _connectionRateLimit.Dispose();

            await base.DisposeAsync();
        }
    }
}
