using Impostor.Hazel.Abstractions;
using Microsoft.Extensions.ObjectPool;
using System;
using System.Buffers.Binary;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;

namespace Impostor.Hazel
{
    public class MessageReader : IMessageReader
    {
        private readonly ObjectPool<MessageReader> _pool;
        private bool _inUse;

        internal MessageReader(ObjectPool<MessageReader> pool)
        {
            _pool = pool;
        }

        public byte[] Buffer { get; private set; }

        public int Offset { get; internal set; }

        public int Position { get; internal set; }

        public int Length { get; internal set; }

        public int BytesRemaining => this.Length - this.Position;

        public byte Tag { get; private set; }

        public MessageReader Parent { get; private set; }

        private int ReadPosition => Offset + Position;
        public void Update(byte[] buffer, int offset = 0, int position = 0, int? length = null, byte tag = byte.MaxValue, MessageReader parent = null)
        {
            _inUse = true;

            Buffer = buffer;
            Offset = offset;
            Position = position;
            Length = length ?? buffer.Length;
            Tag = tag;
            Parent = parent;
        }

        internal void Reset()
        {
            _inUse = false;

            Tag = byte.MaxValue;
            Buffer = null;
            Offset = 0;
            Position = 0;
            Length = 0;
            Parent = null;
        }

        public IMessageReader ReadMessage()
        {
            var startPosition = Position;

            try
            {
                var length = ReadUInt16();
                var tag = FastByte();
                EnsureBytesAvailable(length);

                var pos = ReadPosition;
                Position += length;

                var reader = _pool.Get();
                reader.Update(Buffer, pos, 0, length, tag, this);

                return reader;
            }
            catch (Exception ex)
            {
                Position = startPosition;
                throw CreateReadException("message", startPosition, ex);
            }
        }

        public void RemoveMessage(IMessageReader message)
        {
            if (message.Buffer != Buffer)
            {
                throw new InvalidOperationException("Tried to remove message from a message that does not have the same buffer.");
            }

            // Offset of where to start removing.
            var offsetStart = message.Offset - 3;

            // Offset of where to end removing.
            var offsetEnd = message.Offset + message.Length;

            // The amount of bytes to copy over ourselves.
            var lengthToCopy = message.Buffer.Length - offsetEnd;

            System.Buffer.BlockCopy(Buffer, offsetEnd, Buffer, offsetStart, lengthToCopy);

            ((MessageReader)message).Parent.AdjustLength(message.Offset, message.Length + 3);
        }

        public void InsertMessage(IMessageReader reader, IMessageWriter writer)
        {
            throw new NotImplementedException();
        }

        private void AdjustLength(int offset, int amount)
        {
            if (this.ReadPosition > offset)
            {
                this.Position -= amount;
            }

            this.Length -= amount;

            if (Parent == null)
            {
                // If there's no parent reference, we're at the top-most message
                // and this is not a normal Message, as it either contains no data, or it contains
                // a network reliability header
                return;
            }

            var lengthOffset = this.Offset - 3;
            var curLen = this.Buffer[lengthOffset]
                | (this.Buffer[lengthOffset + 1] << 8);

            curLen -= amount;

            this.Buffer[lengthOffset] = (byte)curLen;
            this.Buffer[lengthOffset + 1] = (byte)(curLen >> 8);

            Parent.AdjustLength(offset, amount);
        }

        public void Dispose()
        {
            if (_inUse)
            {
                _pool.Return(this);
            }
        }

        #region Read Methods
        public bool ReadBoolean()
        {
            byte val = this.FastByte();
            return val != 0;
        }

        public sbyte ReadSByte()
        {
            return (sbyte)this.FastByte();
        }

        public byte ReadByte()
        {
            return this.FastByte();
        }

        public ushort ReadUInt16()
        {
            var output = BinaryPrimitives.ReadUInt16LittleEndian(Buffer.AsSpan(ReadPosition));
            Position += sizeof(ushort);
            return output;
        }

        public short ReadInt16()
        {
            var output = BinaryPrimitives.ReadInt16LittleEndian(Buffer.AsSpan(ReadPosition));
            Position += sizeof(short);
            return output;
        }

        public uint ReadUInt32()
        {
            var output = BinaryPrimitives.ReadUInt32LittleEndian(Buffer.AsSpan(ReadPosition));
            Position += sizeof(uint);
            return output;
        }

        public int ReadInt32()
        {
            var output = BinaryPrimitives.ReadInt32LittleEndian(Buffer.AsSpan(ReadPosition));
            Position += sizeof(int);
            return output;
        }

        public ulong ReadUInt64()
        {
            var output = BinaryPrimitives.ReadUInt64LittleEndian(Buffer.AsSpan(ReadPosition));
            Position += sizeof(ulong);
            return output;
        }

        public long ReadInt64()
        {
            var output = BinaryPrimitives.ReadInt64LittleEndian(Buffer.AsSpan(ReadPosition));
            Position += sizeof(long);
            return output;
        }

        public unsafe float ReadSingle()
        {
            var output = BinaryPrimitives.ReadSingleLittleEndian(Buffer.AsSpan(ReadPosition));
            Position += sizeof(float);
            return output;
        }

        public string ReadString(int length)
        {
            EnsureBytesAvailable(length);

            var output = Encoding.UTF8.GetString(Buffer.AsSpan(ReadPosition, length));
            Position += length;
            return output;
        }

        public string ReadString()
        {
            var startPosition = Position;

            try
            {
                return ReadString(ReadPackedInt32());
            }
            catch (Exception ex)
            {
                Position = startPosition;
                throw CreateReadException("string", startPosition, ex);
            }
        }

        public ReadOnlyMemory<byte> ReadBytesAndSize()
        {
            var startPosition = Position;

            try
            {
                var len = ReadPackedInt32();
                return ReadBytes(len);
            }
            catch (Exception ex)
            {
                Position = startPosition;
                throw CreateReadException("bytes with size", startPosition, ex);
            }
        }

        public ReadOnlyMemory<byte> ReadBytes(int length)
        {
            EnsureBytesAvailable(length);

            var output = Buffer.AsMemory(ReadPosition, length);
            Position += length;
            return output;
        }

        public int ReadPackedInt32()
        {
            return (int)this.ReadPackedUInt32();
        }

        public uint ReadPackedUInt32()
        {
            var startPosition = Position;

            try
            {
                bool readMore = true;
                int shift = 0;
                uint output = 0;

                while (readMore)
                {
                    if (shift >= 35)
                    {
                        throw new InvalidDataException("Packed UInt32 is too large.");
                    }

                    byte b = FastByte();
                    if (b >= 0x80)
                    {
                        readMore = true;
                        b ^= 0x80;
                    }
                    else
                    {
                        readMore = false;
                    }

                    if (shift == 28 && b > 0x0F)
                    {
                        throw new InvalidDataException("Packed UInt32 is too large.");
                    }

                    output |= (uint)(b << shift);
                    shift += 7;
                }

                return output;
            }
            catch (Exception ex)
            {
                Position = startPosition;
                throw CreateReadException("packed UInt32", startPosition, ex);
            }
        }

        #endregion

        public void CopyTo(IMessageWriter writer)
        {
            writer.Write((ushort)Length);
            writer.Write((byte)Tag);
            writer.Write(Buffer.AsMemory(Offset, Length));
        }

        public IMessageReader Copy(int offset = 0)
        {
            var reader = _pool.Get();
            reader.Update(Buffer, Offset + offset, Position, Length - offset, Tag, Parent);
            return reader;
        }

        public void Seek(int position)
        {
            Position = position;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private byte FastByte()
        {
            EnsureBytesAvailable(sizeof(byte));
            return Buffer[Offset + Position++];
        }

        private void EnsureBytesAvailable(int length)
        {
            if (length < 0)
            {
                throw new InvalidDataException($"Read length is negative: {length}");
            }

            if (BytesRemaining < length)
            {
                throw new InvalidDataException($"Read length is longer than message length: {length} of {BytesRemaining}");
            }
        }

        private InvalidDataException CreateReadException(string valueName, int startPosition, Exception innerException)
        {
            return new InvalidDataException(
                $"Failed to read {valueName} at position {startPosition}. Reader was restored to position {Position}. Length: {Length}, bytes remaining: {BytesRemaining}.",
                innerException);
        }
    }
}
