using MemoryPack;
using MemoryPack.Formatters;
using System;
using System.Buffers;

namespace Aevatar.Streams.Kafka.Tests
{
    /// <summary>
    /// Helper class for testing MemoryPack formatters directly.
    /// </summary>
    public class MemberBuilder
    {
        /// <summary>
        /// Format an object using the provided formatter.
        /// </summary>
        public byte[] Format<T>(MemoryPackFormatter<T> formatter, T value)
        {
            var writer = new ArrayBufferWriter<byte>();
            using var optionalState = MemoryPackWriterOptionalStatePool.Rent(MemoryPackSerializerOptions.Default);
            var memoryPackWriter = new MemoryPackWriter<ArrayBufferWriter<byte>>(ref writer, optionalState);
            
            // Reference to value to match the API of MemoryPackFormatter
            var valueRef = value;
            formatter.Serialize(ref memoryPackWriter, ref valueRef);
            memoryPackWriter.Flush();
            
            return writer.WrittenSpan.ToArray();
        }
        
        /// <summary>
        /// Parse a byte array into an object using the provided formatter.
        /// </summary>
        public T Parse<T>(MemoryPackFormatter<T> formatter, byte[] bytes)
        {
            var sequence = new ReadOnlySequence<byte>(bytes);
            var reader = new MemoryPackReader(in sequence, null);
            
            // Create a default value to pass by reference
            T result = default;
            formatter.Deserialize(ref reader, ref result);
            
            return result;
        }
    }
} 