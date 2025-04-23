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
        public Mock<OrleansJsonSerializer> MockSerializer { get; }
        public Mock<ILoggerFactory> MockLoggerFactory { get; }
        public Mock<IGrainFactory> MockGrainFactory { get; }
        public Mock<IExternalStreamDeserializer> MockDeserializer { get; }
        
        public KafkaStreamOptions Options { get; }
        public Dictionary<string, QueueProperties> QueueProperties { get; }
        public string ProviderName { get; }

        public TestFixture()
        {
            MockSerializer = new Mock<OrleansJsonSerializer>();
            MockLoggerFactory = new Mock<ILoggerFactory>();
            MockGrainFactory = new Mock<IGrainFactory>();
            MockDeserializer = new Mock<IExternalStreamDeserializer>();
            
            ProviderName = "TestProvider";
            
            // Create a minimal options instance
            Options = new KafkaStreamOptions();
            
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
} 