using System;
using MemoryPack;
using Orleans.Streams.Utils;
using Orleans.Streams.Utils.Serialization;

namespace Aevatar.Streams.Kafka.Serialization
{
    /// <summary>
    /// Implementation of <see cref="IExternalStreamDeserializer"/> that uses MemoryPack for deserialization.
    /// </summary>
    public class MemoryPackExternalStreamDeserializer : IExternalStreamDeserializer
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="MemoryPackExternalStreamDeserializer"/> class.
        /// </summary>
        public MemoryPackExternalStreamDeserializer()
        {
            // No specific initialization required
        }

        /// <summary>
        /// Deserializes binary data to an object using MemoryPack.
        /// </summary>
        /// <param name="queueProps">Properties of the queue.</param>
        /// <param name="type">The type to deserialize to.</param>
        /// <param name="data">The binary data to deserialize.</param>
        /// <returns>The deserialized object.</returns>
        /// <exception cref="ArgumentNullException">Thrown when data is null.</exception>
        /// <exception cref="ArgumentException">Thrown when type is null.</exception>
        public object Deserialize(QueueProperties queueProps, Type type, byte[] data)
        {
            if (data == null)
                throw new ArgumentNullException(nameof(data));
            
            if (type == null)
                throw new ArgumentException("Type cannot be null", nameof(type));

            try
            {
                // Use MemoryPack to deserialize the data to the specified type
                return MemoryPack.MemoryPackSerializer.Deserialize(type, data);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to deserialize data using MemoryPack: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Disposes resources used by the deserializer.
        /// </summary>
        public void Dispose()
        {
            // No resources to dispose
            GC.SuppressFinalize(this);
        }
    }
} 