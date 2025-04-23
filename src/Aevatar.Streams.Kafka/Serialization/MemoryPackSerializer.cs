using System;
using System.IO;
using System.Collections.Concurrent;
using System.Text;
using MemoryPack;

namespace Aevatar.Streams.Kafka.Serialization
{
    /// <summary>
    /// MemoryPack-based serializer for data serialization
    /// </summary>
    public class MemoryPackSerializer
    {
        private readonly ConcurrentDictionary<Type, object> _emptyInstances = new();
        
        /// <summary>
        /// Serialize an object to string using MemoryPack
        /// </summary>
        public string Serialize(object value, Type type)
        {
            if (value == null)
                return "";
                
            byte[] bytes = MemoryPack.MemoryPackSerializer.Serialize(type, value);
            return Convert.ToBase64String(bytes);
        }
        
        /// <summary>
        /// Deserialize a string to an object using MemoryPack
        /// </summary>
        public object Deserialize(Type type, string serializedValue)
        {
            if (string.IsNullOrEmpty(serializedValue))
                return _emptyInstances.GetOrAdd(type, CreateEmptyInstance);
                
            byte[] bytes = Convert.FromBase64String(serializedValue);
            return MemoryPack.MemoryPackSerializer.Deserialize(type, bytes);
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
    }
} 