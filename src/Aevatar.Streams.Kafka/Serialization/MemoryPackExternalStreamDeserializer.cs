using System;
using System.Collections.Concurrent;
using Orleans.Streams.Utils;
using Orleans.Streams.Utils.Serialization;
using MemoryPack;

namespace Orleans.Streams.Kafka.Serialization
{
    public class MemoryPackExternalStreamDeserializer : IExternalStreamDeserializer
    {
        private readonly ConcurrentDictionary<Type, object> _emptyInstances = new();

        public object Deserialize(QueueProperties queueProps, Type type, byte[] data)
        {
            if (data == null || data.Length == 0)
            {
                return _emptyInstances.GetOrAdd(type, CreateEmptyInstance);
            }

            try
            {
                return MemoryPack.MemoryPackSerializer.Deserialize(type, data);
            }
            catch (Exception ex)
            {
                throw new Exception(
                    $"Failed to deserialize type {type.FullName} from queue {queueProps.QueueName}", 
                    ex);
            }
        }

        private static object CreateEmptyInstance(Type type)
        {
            if (type.IsValueType)
                return Activator.CreateInstance(type);
                
            if (type == typeof(string))
                return string.Empty;
                
            try
            {
                return Activator.CreateInstance(type);
            }
            catch
            {
                return null;
            }
        }

        public void Dispose()
        {
            // Nothing to dispose
        }
    }
} 