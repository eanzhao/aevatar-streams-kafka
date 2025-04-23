using Newtonsoft.Json;
using System;
using System.IO;
using System.Text;
using System.Collections.Concurrent;

namespace Orleans.Streams.Utils.Serialization
{
	public class JsonExternalStreamDeserializer : IExternalStreamDeserializer
	{
		private static readonly JsonSerializerSettings DefaultSettings = new JsonSerializerSettings
		{
			TypeNameHandling = TypeNameHandling.None,
			NullValueHandling = NullValueHandling.Ignore,
			DefaultValueHandling = DefaultValueHandling.Ignore,
			DateFormatHandling = DateFormatHandling.IsoDateFormat,
			DateTimeZoneHandling = DateTimeZoneHandling.Utc,
			Formatting = Formatting.None
		};

		private readonly ConcurrentDictionary<Type, JsonSerializer> _serializers;
		private readonly ConcurrentDictionary<Type, object> _emptyInstances;

		public JsonExternalStreamDeserializer()
		{
			_serializers = new ConcurrentDictionary<Type, JsonSerializer>();
			_emptyInstances = new ConcurrentDictionary<Type, object>();
		}

		public object Deserialize(QueueProperties queueProps, Type type, byte[] data)
		{
			if (data == null || data.Length == 0)
			{
				return _emptyInstances.GetOrAdd(type, CreateEmptyInstance);
			}

			var serializer = _serializers.GetOrAdd(type, t => JsonSerializer.Create(DefaultSettings));

			using (var stream = new MemoryStream(data, writable: false))
			using (var reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: false))
			using (var jsonReader = new JsonTextReader(reader))
			{
				try 
				{
					return serializer.Deserialize(jsonReader, type);
				}
				catch (Exception ex)
				{
					throw new JsonSerializationException(
						$"Failed to deserialize type {type.FullName} from queue {queueProps.QueueName}", 
						ex);
				}
			}
		}

		private static object CreateEmptyInstance(Type type)
		{
			if (type.IsArray)
			{
				return Array.CreateInstance(type.GetElementType(), 0);
			}
			
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
			_serializers.Clear();
			_emptyInstances.Clear();
		}
	};
}