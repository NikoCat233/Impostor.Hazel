using Impostor.Hazel.Abstractions;
using Microsoft.Extensions.ObjectPool;
using Serilog;
using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Impostor.Hazel.Udp
{
    /// <summary>
    ///     Represents a connection that uses the UDP protocol.
    /// </summary>
    /// <inheritdoc />
    public abstract partial class UdpConnection : NetworkConnection
    {
        private static readonly ILogger Logger = Log.ForContext<UdpConnection>();

        /// <summary>
        /// Whether this endpoint supports application-level fragmentation and MTU discovery.
        /// This is a local capability flag.
        /// </summary>
        public bool FragmentationSupported { get; }

        /// <summary>
        /// Whether application-level fragmentation and MTU discovery are enabled for this connection.
        /// This is the negotiated result and is only true when both endpoints support it.
        /// </summary>
        public bool FragmentationEnabled { get; private set; }

        /// <summary>
        /// The remote endpoint's hello version byte (capabilities) as observed from its hello.
        /// </summary>
        public HazelHelloVersion RemoteHelloVersion { get; private set; } = HazelHelloVersion.Legacy;

        public override float AveragePingMs => this._pingMs;

        private const int SioUdpConnectionReset = -1744830452;

        public static readonly byte[] EmptyDisconnectBytes = new byte[] { (byte)UdpSendOption.Disconnect };

        private readonly ConnectionListener _listener;
        protected readonly ObjectPool<MessageReader> _readerPool;
        private readonly CancellationTokenSource _stoppingCts;

        private bool _isDisposing;
        private bool _isFirst = true;
        private Task _executingTask;

        protected UdpConnection(ConnectionListener listener, ObjectPool<MessageReader> readerPool, bool enableFragmentation = false)
        {
            _listener = listener;
            _readerPool = readerPool;
            _stoppingCts = new CancellationTokenSource();

            FragmentationSupported = enableFragmentation;

            // Negotiated later (via hello version exchange). Default to legacy behaviour.
            FragmentationEnabled = false;

            Pipeline = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = true
            });
        }

        internal void SetRemoteHelloVersion(byte ver)
        {
            RemoteHelloVersion = (HazelHelloVersion)ver;

            var wasEnabled = FragmentationEnabled;
            FragmentationEnabled =
                FragmentationSupported &&
                RemoteHelloVersion >= HazelHelloVersion.Fragmentation;

            // Start MTU discovery as soon as fragmentation becomes enabled on a connected socket.
            if (!wasEnabled && FragmentationEnabled && _state == ConnectionState.Connected)
            {
                StartMtuDiscovery();
            }
        }

        /// <summary>
        /// Sends a hello response containing only this endpoint's hello version byte.
        /// This is used for capability negotiation so that the client can learn the server's capability.
        /// </summary>
        internal ValueTask SendHelloResponseAsync()
        {
            // Empty payload (only the version byte is sent).
            return SendHello(null, null);
        }

        internal Channel<byte[]> Pipeline { get; }

        public Task StartAsync()
        {
            // Store the task we're executing
            _executingTask = Task.Factory.StartNew(ReadAsync, TaskCreationOptions.LongRunning);

            // If the task is completed then return it, this will bubble cancellation and failure to the caller
            if (_executingTask.IsCompleted)
            {
                return _executingTask;
            }

            // Otherwise it's running
            return Task.CompletedTask;
        }

        public void Stop()
        {
            // Stop called without start
            if (_executingTask == null)
            {
                return;
            }

            // Signal cancellation to methods.
            _stoppingCts.Cancel();

            try
            {
                // Cancel reader.
                Pipeline.Writer.Complete();
            }
            catch (ChannelClosedException)
            {
                // Already done.
            }

            // Remove references.
            if (!_isDisposing)
            {
                Dispose(true);
            }
        }

        private async Task ReadAsync()
        {
            var reader = new MessageReader(_readerPool);

            while (!_stoppingCts.IsCancellationRequested)
            {
                var result = await Pipeline.Reader.ReadAsync(_stoppingCts.Token);

                try
                {
                    reader.Update(result);

                    await HandleReceive(reader, reader.Length);
                }
                catch (Exception e)
                {
                    Logger.Error(e, "Exception during ReadAsync");
                    Dispose(true);
                    break;
                }
            }
        }

        /// <summary>
        ///     Writes the given bytes to the connection.
        /// </summary>
        /// <param name="bytes">The bytes to write.</param>
        protected abstract ValueTask WriteBytesToConnection(byte[] bytes, int length, Action<SocketException> onError = null);

        /// <inheritdoc/>
        public override async ValueTask SendAsync(IMessageWriter msg)
        {
            if (this._state != ConnectionState.Connected)
                throw new InvalidOperationException("Could not send data as this Connection is not connected. Did you disconnect?");

            // Optional application-level fragmentation and MTU discovery.
            // When enabled, we can fragment large reliable packets. For oversized unreliable packets
            // we only log a warning and attempt to send without killing the connection.
            var isOversizeUnreliable = this.FragmentationEnabled
                && msg.SendOption != MessageType.Reliable
                && msg.Length > this.Mtu;

            if (this.FragmentationEnabled && msg.SendOption == MessageType.Reliable && msg.Length > this.Mtu)
            {
                ResetKeepAliveTimer();
                await FragmentedSend((byte)MessageType.Reliable, msg.ToByteArray(false));
                return;
            }
            byte[] buffer = new byte[msg.Length];
            Buffer.BlockCopy(msg.Buffer, 0, buffer, 0, msg.Length);

            switch (msg.SendOption)
            {
                case MessageType.Reliable:
                    ResetKeepAliveTimer();

                    AttachReliableID(buffer, 1);
                    await WriteBytesToConnection(buffer, buffer.Length);
                    Statistics.LogReliableSend(buffer.Length - 3, buffer.Length);
                    break;

                default:
                    if (isOversizeUnreliable)
                    {
                        Logger.Warning("Attempted to send unreliable message of size {Size} which exceeds MTU {Mtu}. The packet may be dropped.", buffer.Length, this.Mtu);
                        await WriteBytesToConnection(buffer, buffer.Length, _ => { });
                    }
                    else
                    {
                        await WriteBytesToConnection(buffer, buffer.Length);
                    }
                    Statistics.LogUnreliableSend(buffer.Length - 1, buffer.Length);
                    break;
            }
        }

        /// <inheritdoc/>
        /// <remarks>
        ///     <include file="DocInclude/common.xml" path="docs/item[@name='Connection_SendBytes_General']/*" />
        ///     <para>
        ///         Udp connections can currently send messages using <see cref="MessageType.Unreliable"/> and
        ///         <see cref="MessageType.Reliable"/>. When fragmentation is enabled, reliable messages larger than
        ///         the connection MTU will be fragmented. Oversized unreliable messages will log a warning and may be dropped.
        ///     </para>
        /// </remarks>
        public override async ValueTask SendBytes(byte[] bytes, MessageType sendOption = MessageType.Unreliable)
        {
            //Add header information and send
            await HandleSend(bytes, (byte)sendOption);
        }

        /// <summary>
        ///     Handles the reliable/fragmented sending from this connection.
        /// </summary>
        /// <param name="data">The data being sent.</param>
        /// <param name="sendOption">The <see cref="SendOption"/> specified as its byte value.</param>
        /// <param name="ackCallback">The callback to invoke when this packet is acknowledged.</param>
        /// <returns>The bytes that should actually be sent.</returns>
        protected async ValueTask HandleSend(byte[] data, byte sendOption, Action ackCallback = null)
        {
            // Fragment large reliable messages.
            if (this.FragmentationEnabled && sendOption == (byte)MessageType.Reliable && (data.Length + 3) > this.Mtu)
            {
                await FragmentedSend(sendOption, data, ackCallback);
                return;
            }

            switch (sendOption)
            {
                case (byte)UdpSendOption.Ping:
                case (byte)MessageType.Reliable:
                case (byte)UdpSendOption.Hello:
                case (byte)UdpSendOption.MtuTest:
                    await ReliableSend(sendOption, data, ackCallback);
                    break;

                //Treat all else as unreliable
                default:
                    await UnreliableSend(sendOption, data);
                    break;
            }
        }

        /// <summary>
        ///     Handles the receiving of data.
        /// </summary>
        /// <param name="message">The buffer containing the bytes received.</param>
        internal virtual async ValueTask HandleReceive(MessageReader message, int bytesReceived)
        {
            // Check if the first message received is the hello packet.
            if (_isFirst)
            {
                _isFirst = false;

                // Negotiate capabilities using the hello version byte.
                // Layout: [SendOption(1)][ReliableId(2)][HelloVersion(1)]...
                if (bytesReceived >= 4 && message.Buffer[0] == (byte)UdpSendOption.Hello)
                {
                    SetRemoteHelloVersion(message.Buffer[3]);
                }

                // Slice 4 bytes to get handshake data.
                if (_listener != null)
                {
                    using (var handshake = message.Copy(4))
                    {
                        await _listener.InvokeNewConnection(handshake, this);
                    }
                }
            }

            switch (message.Buffer[0])
            {
                //Handle reliable receives
                case (byte)MessageType.Reliable:
                    await ReliableMessageReceive(message, bytesReceived);
                    break;

                //Handle acknowledgments
                case (byte)UdpSendOption.Acknowledgement:
                    AcknowledgementMessageReceive(message.Buffer, bytesReceived);
                    break;

                //We need to acknowledge hello and ping messages but dont want to invoke any events!
                case (byte)UdpSendOption.Ping:
                    await ProcessReliableReceive(message.Buffer, 1);
                    Statistics.LogHelloReceive(bytesReceived);
                    break;
                case (byte)UdpSendOption.Hello:
                    // Hello carries the remote capability byte as the first byte of its payload.
                    // Layout: [SendOption(1)][ReliableId(2)][HelloVersion(1)]...
                    if (bytesReceived >= 4)
                    {
                        SetRemoteHelloVersion(message.Buffer[3]);
                    }
                    await ProcessReliableReceive(message.Buffer, 1);
                    Statistics.LogHelloReceive(bytesReceived);
                    break;

                case (byte)UdpSendOption.MtuTest:
                    // We can safely process MTU test messages as long as we support the feature locally.
                    // (The negotiated flag only gates what we send.)
                    if (this.FragmentationSupported)
                    {
                        await MtuTestMessageReceive(message);
                    }
                    else
                    {
                        // Ignore MTU test messages when fragmentation is disabled.
                        Statistics.LogUnreliableReceive(bytesReceived - 1, bytesReceived);
                    }
                    break;

                case (byte)UdpSendOption.Fragment:
                    // We can safely process fragments as long as we support the feature locally.
                    // (The negotiated flag only gates what we send.)
                    if (this.FragmentationSupported)
                    {
                        await FragmentMessageReceive(message, bytesReceived);
                    }
                    else
                    {
                        // Ignore fragments when fragmentation is disabled.
                        Statistics.LogUnreliableReceive(bytesReceived - 1, bytesReceived);
                    }
                    break;

                case (byte)UdpSendOption.Disconnect:
                    message.Offset = 1;
                    message.Position = 0;
                    await DisconnectRemote("The remote sent a disconnect request", message);
                    break;

                //Treat everything else as unreliable
                default:
                    await InvokeDataReceived(MessageType.Unreliable, message, 1, bytesReceived);
                    Statistics.LogUnreliableReceive(bytesReceived - 1, bytesReceived);
                    break;
            }
        }

        /// <summary>
        ///     Sends bytes using the unreliable UDP protocol.
        /// </summary>
        /// <param name="sendOption">The SendOption to attach.</param>
        /// <param name="data">The data.</param>
        ValueTask UnreliableSend(byte sendOption, byte[] data)
        {
            return this.UnreliableSend(sendOption, data, 0, data.Length);
        }

        /// <summary>
        ///     Sends bytes using the unreliable UDP protocol.
        /// </summary>
        /// <param name="data">The data.</param>
        /// <param name="sendOption">The SendOption to attach.</param>
        /// <param name="offset"></param>
        /// <param name="length"></param>
        async ValueTask UnreliableSend(byte sendOption, byte[] data, int offset, int length)
        {
            var isOversize = this.FragmentationEnabled && (length + 1 > this.Mtu);
            if (isOversize)
            {
                Logger.Warning("Attempted to send unreliable message of size {Size} which exceeds MTU {Mtu}. The packet may be dropped.", length + 1, this.Mtu);
            }

            byte[] bytes = new byte[length + 1];

            //Add message type
            bytes[0] = sendOption;

            //Copy data into new array
            Buffer.BlockCopy(data, offset, bytes, bytes.Length - length, length);

            // Write to connection.
            if (isOversize)
            {
                // Provide an error handler so we don't disconnect on MessageSize errors.
                await WriteBytesToConnection(bytes, bytes.Length, _ => { });
            }
            else
            {
                await WriteBytesToConnection(bytes, bytes.Length);
            }

            Statistics.LogUnreliableSend(length, bytes.Length);
        }

        /// <summary>
        ///     Helper method to invoke the data received event.
        /// </summary>
        /// <param name="sendOption">The send option the message was received with.</param>
        /// <param name="buffer">The buffer received.</param>
        /// <param name="dataOffset">The offset of data in the buffer.</param>
        ValueTask InvokeDataReceived(MessageType sendOption, MessageReader buffer, int dataOffset, int bytesReceived)
        {
            buffer.Offset = dataOffset;
            buffer.Length = bytesReceived - dataOffset;
            buffer.Position = 0;

            return InvokeDataReceived(buffer, sendOption);
        }

        /// <summary>
        ///     Sends a hello packet to the remote endpoint.
        /// </summary>
        /// <param name="acknowledgeCallback">The callback to invoke when the hello packet is acknowledged.</param>
        protected ValueTask SendHello(byte[] bytes, Action acknowledgeCallback)
        {
            //First byte of handshake is version indicator so add data after
            byte[] actualBytes;
            if (bytes == null)
            {
                actualBytes = new byte[1];
            }
            else
            {
                actualBytes = new byte[bytes.Length + 1];
                Buffer.BlockCopy(bytes, 0, actualBytes, 1, bytes.Length);
            }

            // Write our hello version/capability byte.
            actualBytes[0] = this.FragmentationSupported
                ? (byte)HazelHelloVersion.Fragmentation
                : (byte)HazelHelloVersion.Legacy;

            return HandleSend(actualBytes, (byte)UdpSendOption.Hello, acknowledgeCallback);
        }

        /// <inheritdoc/>
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _isDisposing = true;

                Stop();
                DisposeKeepAliveTimer();
                DisposeReliablePackets();
            }

            base.Dispose(disposing);
        }
    }
}
