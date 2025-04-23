using Microsoft.Extensions.Logging;
using Moq;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Config;
using Orleans.Streams.Utils;
using Orleans.Streams.Utils.Serialization;
using System;
using System.Collections.Generic;

namespace Aevatar.Streams.Kafka.Tests
{
    public class TestFixture : IDisposable
    {
        // Use interfaces instead of concrete classes for easier mocking
        public Mock<IOrleansJsonSerializer> MockSerializer { get; }
        public Mock<ILoggerFactory> MockLoggerFactory { get; }
        public Mock<IGrainFactory> MockGrainFactory { get; }
        public Mock<IExternalStreamDeserializer> MockDeserializer { get; }
        
        public KafkaStreamOptions Options { get; }
        public Dictionary<string, QueueProperties> QueueProperties { get; }
        public string ProviderName { get; }

        public TestFixture()
        {
            // Use interface for serializer rather than concrete class
            MockSerializer = new Mock<IOrleansJsonSerializer>();
            MockLoggerFactory = new Mock<ILoggerFactory>();
            MockGrainFactory = new Mock<IGrainFactory>();
            MockDeserializer = new Mock<IExternalStreamDeserializer>();
            
            // Fix for the extension method mocking issue
            // Instead of using SetupXXX, we use Returns with a callback function
            MockLoggerFactory.Setup(x => x.CreateLogger(It.IsAny<string>()))
                .Returns((string categoryName) => {
                    if (categoryName == typeof(Orleans.Streams.Kafka.Core.KafkaAdapterReceiver).FullName)
                    {
                        return new Mock<ILogger<Orleans.Streams.Kafka.Core.KafkaAdapterReceiver>>().Object;
                    }
                    if (categoryName == typeof(Orleans.Streams.Kafka.Core.KafkaAdapter).FullName)
                    {
                        return new Mock<ILogger<Orleans.Streams.Kafka.Core.KafkaAdapter>>().Object;
                    }
                    return new Mock<ILogger>().Object;
                });
            
            ProviderName = "TestProvider";
            
            // Create a minimal options instance
            Options = new KafkaStreamOptions
            {
                BrokerList = new List<string> { "localhost:9092" },
                InitialBatchSize = 20,
                MaxBatchSize = 100,
                MinBatchSize = 10,
                BatchSizeAdjustmentFactor = 1.5,
                BatchSizeAdjustmentInterval = TimeSpan.FromSeconds(5),
                EnableAdaptiveBatching = true,
                MessageTrackingEnabled = true,
                PollTimeout = TimeSpan.FromMilliseconds(100),
                PollBufferTimeout = TimeSpan.FromMilliseconds(200),
                ConsumerGroupId = "test-group"
            };
            
            // Set up a basic queue property
            QueueProperties = new Dictionary<string, QueueProperties>
            {
                { "test-namespace", new QueueProperties("test-namespace", 0) }
            };
        }

        public void Dispose()
        {
            // Cleanup any resources if needed
        }
    }

    // Create an interface that matches the methods we need from OrleansJsonSerializer
    // This makes it mockable without needing a parameterless constructor
    public interface IOrleansJsonSerializer
    {
        object Deserialize(byte[] data);
        byte[] Serialize(object item);
    }
} 