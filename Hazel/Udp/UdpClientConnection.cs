using Impostor.Hazel.Abstractions;
using Microsoft.Extensions.ObjectPool;
using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Impostor.Hazel.Udp
{
    /// <summary>
    ///     Represents a client's connection to a server that uses the UDP protocol.
    /// </summary>
    /// <inheritdoc/>
    public class UdpClientConnection : UdpConnection
    {
        /// <summary>
        ///     The socket we're connected via.
        /// </summary>
        private readonly UdpClient _socket;

        /// <summary>
        ///     Reset event that is triggered when the connection is marked Connected.
        /// </summary>
        private readonly SemaphoreSlim _connectWaitLock;

        private Task _listenTask;

        private Timer reliablePacketTimer;

        /// <summary>
        ///     Creates a new UdpClientConnection.
        /// </summary>
        /// <param name="remoteEndPoint">A <see cref="NetworkEndPoint"/> to connect to.</param>
        public UdpClientConnection(IPEndPoint remoteEndPoint, ObjectPool<MessageReader> readerPool, IPMode ipMode = IPMode.IPv4, bool enableFragmentation = false)
            : base(null, readerPool, enableFragmentation)
        {
            this.EndPoint = remoteEndPoint;
            this.IPMode = ipMode;

            _socket = new UdpClient();

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

            reliablePacketTimer = new Timer(ManageReliablePacketsInternal, null, 100, Timeout.Infinite);
            _connectWaitLock = new SemaphoreSlim(0, 1);

            this.InitializeKeepAliveTimer();
        }

        ~UdpClientConnection()
        {
            this.Dispose(false);
        }

        private async void ManageReliablePacketsInternal(object state)
        {
            await base.ManageReliablePackets();
            try
            {
                reliablePacketTimer.Change(100, Timeout.Infinite);
            }
            catch { }
        }

        protected virtual async ValueTask ResendPacketsIfNeeded()
        {
            await base.ManageReliablePackets();
        }

        /// <inheritdoc />
        protected override ValueTask WriteBytesToConnection(byte[] bytes, int length, Action<SocketException> onError = null)
        {
            return WriteBytesToConnectionReal(bytes, length, onError);
        }

        private async ValueTask WriteBytesToConnectionReal(byte[] bytes, int length, Action<SocketException> onError)
        {
            try
            {
                await _socket.SendAsync(bytes, length);
            }
            catch (NullReferenceException) { }
            catch (ObjectDisposedException)
            {
                // Already disposed and disconnected...
            }
            catch (SocketException ex)
            {
                if (onError != null)
                {
                    onError(ex);
                    return;
                }

                await DisconnectInternal(HazelInternalErrors.SocketExceptionSend, "Could not send data as a SocketException occurred: " + ex.Message);
            }
        }

        /// <inheritdoc />
        public override async ValueTask ConnectAsync(byte[] bytes = null, int timeout = 5000)
        {
            State = ConnectionState.Connecting;

            try
            {
                _socket.Connect(EndPoint);
            }
            catch (SocketException e)
            {
                State = ConnectionState.NotConnected;
                throw new HazelException("A SocketException occurred while binding to the port.", e);
            }

            this.RestartConnection();

            try
            {
                _listenTask = Task.Factory.StartNew(ListenAsync, TaskCreationOptions.LongRunning);
            }
            catch (ObjectDisposedException)
            {
                // If the socket's been disposed then we can just end there but make sure we're in NotConnected state.
                // If we end up here I'm really lost...
                State = ConnectionState.NotConnected;
                return;
            }
            catch (SocketException e)
            {
                Dispose();
                throw new HazelException("A SocketException occurred while initiating a receive operation.", e);
            }

            // Write bytes to the server to tell it hi (and to punch a hole in our NAT, if present)
            // When acknowledged set the state to connected
            await SendHello(bytes, () =>
            {
                State = ConnectionState.Connected;
                InitializeKeepAliveTimer();
                StartMtuDiscovery();
            });

            await _connectWaitLock.WaitAsync(TimeSpan.FromSeconds(10));
        }

        protected virtual void RestartConnection()
        {
        }

        private async Task ListenAsync()
        {
            // Start packet handler.
            await StartAsync();

            // Listen.
            while (State != ConnectionState.NotConnected)
            {
                UdpReceiveResult data;

                try
                {
                    data = await _socket.ReceiveAsync();
                }
                catch (SocketException e)
                {
                    await DisconnectInternal(HazelInternalErrors.SocketExceptionReceive, "Socket exception while reading data: " + e.Message);
                    return;
                }
                catch (Exception)
                {
                    return;
                }

                if (data.Buffer.Length == 0)
                {
                    await DisconnectInternal(HazelInternalErrors.ReceivedZeroBytes, "Received 0 bytes");
                    return;
                }

                // Write to client.
                await Pipeline.Writer.WriteAsync(data.Buffer);
            }
        }

        protected override void SetState(ConnectionState state)
        {
            if (state == ConnectionState.Connected)
            {
                _connectWaitLock.Release();
            }
        }
        /// <summary>
        ///     Sends a disconnect message to the end point.
        ///     You may include optional disconnect data. The SendOption must be unreliable.
        /// </summary>
        protected override async ValueTask<bool> SendDisconnect(MessageWriter data = null)
        {
            lock (this)
            {
                if (this._state == ConnectionState.NotConnected) return false;
                this._state = ConnectionState.NotConnected;
            }

            var bytes = EmptyDisconnectBytes;
            if (data != null && data.Length > 0)
            {
                if (data.SendOption != MessageType.Unreliable) throw new ArgumentException("Disconnect messages can only be unreliable.");

                bytes = data.ToByteArray(true);
                bytes[0] = (byte)UdpSendOption.Disconnect;
            }

            try
            {
                await _socket.SendAsync(bytes, bytes.Length, EndPoint);
            }
            catch { }

            return true;
        }

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            State = ConnectionState.NotConnected;

            try { _socket.Close(); }
            catch { }

            try { _socket.Dispose(); }
            catch { }

            reliablePacketTimer.Dispose();
            _connectWaitLock.Dispose();

            base.Dispose(disposing);
        }
    }
}
