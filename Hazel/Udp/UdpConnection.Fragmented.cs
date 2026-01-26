using Impostor.Hazel.Abstractions;
using System;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Impostor.Hazel.Udp
{
    public partial class UdpConnection
    {
        /// <summary>
        /// Maximum possible UDP header size - 60-byte IP header + 8-byte UDP header.
        /// </summary>
        public const ushort MaxUdpHeaderSize = 68;

        /// <summary>
        /// Popular MTU values used for quick MTU discovery.
        /// </summary>
        public static ushort[] PossibleMtu { get; } =
        {
            576 - MaxUdpHeaderSize, // RFC 1191
            1024,
            1460 - MaxUdpHeaderSize, // Google Cloud
            1492 - MaxUdpHeaderSize, // RFC 1042
            1500 - MaxUdpHeaderSize, // RFC 1191
        };

        private int _mtu = PossibleMtu[0];
        private byte _mtuIndex;
        private volatile bool _mtuDiscoveryStarted;

        /// <summary>
        /// MTU of this connection.
        /// </summary>
        public int Mtu => ForcedMtu ?? _mtu;

        /// <summary>
        /// Forced MTU, overrides the discovered MTU.
        /// </summary>
        public int? ForcedMtu { get; set; } = null;

        /// <summary>
        ///     Called when the MTU changes.
        /// </summary>
        public event Action MtuChanged;

        private readonly ConcurrentDictionary<ushort, FragmentedMessage> _fragmentedMessagesReceived = new();
        private volatile int _lastFragmentedId;

        protected void StartMtuDiscovery()
        {
            if (!this.FragmentationEnabled)
            {
                return;
            }

            if (_mtuDiscoveryStarted || ForcedMtu.HasValue)
            {
                return;
            }

            _mtuDiscoveryStarted = true;
            _ = MtuTestAsync(_mtuIndex);
        }

        private async ValueTask MtuTestAsync(byte index)
        {
            var mtu = PossibleMtu[index];
            var failed = false;

            var buffer = new byte[mtu];
            buffer[0] = (byte)UdpSendOption.MtuTest;

            var id = AttachReliableID(buffer, 1, () =>
            {
                if (failed) return;
                MtuOk(index);
            });

            buffer[mtu - 2] = (byte)mtu;
            buffer[mtu - 1] = (byte)(mtu >> 8);

            await WriteBytesToConnection(buffer, buffer.Length, async (SocketException _) =>
            {
                failed = true;
                CancelReliableMessageId(id);

                if (index == 0)
                {
                    await DisconnectInternal(HazelInternalErrors.ConnectionDisconnected, "Connection MTU is lower than the minimum");
                }
            });
        }

        private void MtuOk(byte index)
        {
            _mtuIndex = index;
            _mtu = PossibleMtu[index];
            MtuChanged?.Invoke();

            if (_mtuIndex < PossibleMtu.Length - 1)
            {
                _ = MtuTestAsync((byte)(index + 1));
            }
        }

        private async ValueTask MtuTestMessageReceive(MessageReader message)
        {
            message.Position = message.Length - 2;
            var mtu = message.ReadUInt16();
            if (mtu != message.Length)
            {
                return;
            }

            await ProcessReliableReceive(message.Buffer, 1);
        }

        // UdpSendOption.Fragment + ReliableID (2) + MessageId (2) + FragmentsCount (1) + FragmentId (1)
        private const byte FragmentHeaderSize = sizeof(byte) + sizeof(ushort) + sizeof(ushort) + sizeof(byte) + sizeof(byte);

        /// <summary>
        /// Fragments and sends a reliable message.
        /// </summary>
        protected async ValueTask FragmentedSend(byte sendOption, byte[] data, Action ackCallback = null)
        {
            // Inform keepalive not to send for a while.
            ResetKeepAliveTimer();

            var length = data.Length + 1; // +1 for sendOption stored in the first fragment payload

            var id = (ushort)Interlocked.Increment(ref _lastFragmentedId);
            var fragmentSize = Mtu;
            var fragmentDataSize = fragmentSize - FragmentHeaderSize;

            if (fragmentDataSize <= 1)
            {
                throw new HazelException("MTU is too low to support fragmentation");
            }

            var fragmentsCount = (int)Math.Ceiling(length / (double)fragmentDataSize);
            if (fragmentsCount > byte.MaxValue)
            {
                throw new HazelException("Too many fragments");
            }

            var acksReceived = 0;

            for (byte i = 0; i < fragmentsCount; i++)
            {
                var dataLength = Math.Min(fragmentDataSize, length - fragmentDataSize * i);

                var bytes = new byte[dataLength + FragmentHeaderSize];

                // Add message type.
                bytes[0] = (byte)UdpSendOption.Fragment;

                // Add reliable ID.
                AttachReliableID(bytes, 1, () =>
                {
                    if (Interlocked.Increment(ref acksReceived) >= fragmentsCount)
                    {
                        ackCallback?.Invoke();
                    }
                });

                // Fragmented message ID (little endian).
                bytes[3] = (byte)id;
                bytes[4] = (byte)(id >> 8);

                bytes[5] = (byte)fragmentsCount;
                bytes[6] = i;

                var includingHeader = i == 0;
                if (includingHeader)
                {
                    bytes[7] = sendOption;
                }

                Buffer.BlockCopy(
                    data,
                    fragmentDataSize * i - (includingHeader ? 0 : 1),
                    bytes,
                    FragmentHeaderSize + (includingHeader ? 1 : 0),
                    dataLength - (includingHeader ? 1 : 0));

                await WriteBytesToConnection(bytes, bytes.Length);

                // Count user payload bytes only (exclude sendOption stored in the first fragment)
                Statistics.LogFragmentedSend(dataLength - (includingHeader ? 1 : 0), bytes.Length);
            }
        }

        private async ValueTask FragmentMessageReceive(MessageReader messageReader, int bytesReceived)
        {
            var isNew = await ProcessReliableReceive(messageReader.Buffer, 1);

            messageReader.Position = 3;
            var fragmentedMessageId = messageReader.ReadUInt16();
            var fragmentsCount = messageReader.ReadByte();
            var fragmentId = messageReader.ReadByte();

            if (fragmentsCount <= 0 || fragmentId >= fragmentsCount)
            {
                return;
            }

            var fragmentPayloadLen = bytesReceived - messageReader.Position;
            var userBytes = fragmentPayloadLen - (fragmentId == 0 ? 1 : 0);
            Statistics.LogFragmentedReceive(userBytes, bytesReceived);

            if (!isNew)
            {
                return;
            }

            FragmentedMessage fragmentedMessage = _fragmentedMessagesReceived.GetOrAdd(fragmentedMessageId, _ => new FragmentedMessage(fragmentsCount));
            bool isFinished = false;
            byte[] reconstructedBytes = null;

            lock (fragmentedMessage)
            {
                if (fragmentedMessage.Fragments[fragmentId] != null)
                {
                    return;
                }

                var buffer = new byte[fragmentPayloadLen];
                Buffer.BlockCopy(messageReader.Buffer, messageReader.Offset + messageReader.Position, buffer, 0, fragmentPayloadLen);
                fragmentedMessage.AddFragment(fragmentId, buffer);

                if (fragmentedMessage.IsFinished)
                {
                    reconstructedBytes = fragmentedMessage.Reconstruct();
                    _fragmentedMessagesReceived.TryRemove(fragmentedMessageId, out _);
                    isFinished = true;
                }
            }

            if (isFinished && reconstructedBytes != null)
            {
                var reconstructed = _readerPool.Get();
                reconstructed.Update(reconstructedBytes);

                await InvokeDataReceived((MessageType)reconstructedBytes[0], reconstructed, 1, reconstructedBytes.Length);
            }
        }

        private sealed class FragmentedMessage
        {
            public int FragmentsCount { get; }
            public int FragmentsReceived { get; private set; }
            public int Size { get; private set; }
            public byte[][] Fragments { get; }
            public bool IsFinished => FragmentsReceived == FragmentsCount;

            public FragmentedMessage(int fragmentsCount)
            {
                FragmentsCount = fragmentsCount;
                Fragments = new byte[fragmentsCount][];
            }

            public void AddFragment(byte id, byte[] fragment)
            {
                Fragments[id] = fragment;
                Size += fragment.Length;
                FragmentsReceived++;
            }

            public byte[] Reconstruct()
            {
                if (!IsFinished)
                {
                    throw new HazelException("Can't reconstruct a FragmentedMessage until all fragments are received");
                }

                var buffer = new byte[Size];
                var offset = 0;
                for (var i = 0; i < FragmentsCount; i++)
                {
                    var data = Fragments[i];
                    Buffer.BlockCopy(data, 0, buffer, offset, data.Length);
                    offset += data.Length;
                }

                return buffer;
            }
        }
    }
}
