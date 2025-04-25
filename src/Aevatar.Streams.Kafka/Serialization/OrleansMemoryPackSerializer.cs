using System;
using System.Net;
using System.Text;
using System.Buffers;
using Microsoft.Extensions.Options;
using Orleans.Runtime;
using Orleans.GrainReferences;
using MemoryPack;
using System.Collections.Concurrent;

namespace Orleans.Serialization
{
    /// <summary>
    /// Utility class for serializing Orleans types using MemoryPack.
    /// </summary>
    public class OrleansMemoryPackSerializer
    {
        private readonly ConcurrentDictionary<Type, object> _emptyInstances = new();
        private readonly OrleansMemoryPackSerializerOptions _options;
        
        /// <summary>
        /// Initializes a new instance of the <see cref="OrleansMemoryPackSerializer"/> class.
        /// </summary>
        public OrleansMemoryPackSerializer(IOptions<OrleansMemoryPackSerializerOptions> options)
        {
            _options = options.Value;
            
            // Register formatters will happen in the MemoryPack initialization/configuration
            // This would typically be done in a service configuration
        }
        
        /// <summary>
        /// Deserializes an object of the specified expected type from the provided input.
        /// </summary>
        /// <param name="expectedType">The expected type.</param>
        /// <param name="input">The input string (Base64 encoded).</param>
        /// <returns>The deserialized object.</returns>
        public object Deserialize(Type expectedType, string input)
        {
            if (string.IsNullOrWhiteSpace(input))
            {
                return _emptyInstances.GetOrAdd(expectedType, CreateEmptyInstance);
            }

            byte[] bytes = Convert.FromBase64String(input);
            return MemoryPackSerializer.Deserialize(expectedType, bytes);
        }

        /// <summary>
        /// Serializes an object to a Base64 encoded string.
        /// </summary>
        /// <param name="item">The object to serialize.</param>
        /// <param name="expectedType">The type the deserializer should expect.</param>
        public string Serialize(object item, Type expectedType)
        {
            if (item == null)
                return string.Empty;
                
            byte[] bytes = MemoryPackSerializer.Serialize(expectedType, item);
            return Convert.ToBase64String(bytes);
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
    
    /// <summary>
    /// Options for configuring the Orleans MemoryPack serializer.
    /// </summary>
    public class OrleansMemoryPackSerializerOptions
    {
        /// <summary>
        /// Gets or sets the grain reference activator.
        /// </summary>
        public GrainReferenceActivator ReferenceActivator { get; set; }
    }
} 