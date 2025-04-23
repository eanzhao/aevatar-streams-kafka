using Confluent.Kafka;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Core;
using System.Text;
using Aevatar.Streams.Kafka.Serialization;

namespace Orleans.Streams.Kafka.Serialization
{
    internal class KafkaBatchContainerMemoryPackSerializer : ISerializer<KafkaBatchContainer>
    {
        private readonly MemoryPackSerializer _serializer;

        public KafkaBatchContainerMemoryPackSerializer(MemoryPackSerializer serializer)
        {
            _serializer = serializer;
        }

        public byte[] Serialize(KafkaBatchContainer data, Confluent.Kafka.SerializationContext context)
        {
            var serializedString = _serializer.Serialize(data, typeof(KafkaBatchContainer));
            return Encoding.UTF8.GetBytes(serializedString);
        }
    }
} 