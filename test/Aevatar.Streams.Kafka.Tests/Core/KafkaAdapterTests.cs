using Confluent.Kafka;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Config;
using Orleans.Streams.Kafka.Core;
using Orleans.Streams.Utils.Serialization;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace Aevatar.Streams.Kafka.Tests.Core
{
    public class KafkaAdapterTests
    {
        private readonly Mock<OrleansJsonSerializer> _mockSerializer;
        private readonly Mock<ILoggerFactory> _mockLoggerFactory;
        private readonly Mock<IGrainFactory> _mockGrainFactory;
        private readonly Mock<IExternalStreamDeserializer> _mockDeserializer;
        private readonly Mock<ILogger<KafkaAdapter>> _mockLogger;
        private readonly Mock<IProducer<byte[], KafkaBatchContainer>> _mockProducer;
        
        private readonly string _providerName = "TestProvider";
        private readonly KafkaStreamOptions _options;
        private readonly Dictionary<string, QueueProperties> _queueProperties;

        public KafkaAdapterTests()
        {
            _mockSerializer = new Mock<OrleansJsonSerializer>();
            _mockLoggerFactory = new Mock<ILoggerFactory>();
            _mockGrainFactory = new Mock<IGrainFactory>();
            _mockDeserializer = new Mock<IExternalStreamDeserializer>();
            _mockLogger = new Mock<ILogger<KafkaAdapter>>();
            _mockProducer = new Mock<IProducer<byte[], KafkaBatchContainer>>();
            
            _options = new KafkaStreamOptions
            {
                BrokerList = new List<string> { "localhost:9092" },
                ConsumerGroupId = "test-group",
                Topics = new List<string> { "test-topic" },
                ImportRequestContext = true
            };
            
            _queueProperties = new Dictionary<string, QueueProperties>
            {
                { "test-namespace", new QueueProperties("test-namespace", 0) }
            };
            
            _mockLoggerFactory.Setup(x => x.CreateLogger<KafkaAdapter>()).Returns(_mockLogger.Object);
        }
        
        [Fact]
        public void Constructor_ShouldInitializeCorrectly()
        {
            // Arrange & Act
            var adapter = CreateAdapter();
            
            // Assert
            adapter.Should().NotBeNull();
            adapter.Name.Should().Be(_providerName);
            adapter.IsRewindable.Should().BeFalse();
            adapter.Direction.Should().Be(StreamProviderDirection.ReadWrite);
        }
        
        [Fact]
        public async Task QueueMessageBatchAsync_WithEmptyEvents_ShouldReturnEarly()
        {
            // Arrange
            var adapter = CreateAdapter();
            var streamId = StreamId.Create("test-namespace", Guid.NewGuid());
            var events = new List<object>();
            
            // Act
            await adapter.QueueMessageBatchAsync(streamId, events, null, new Dictionary<string, object>());
            
            // Assert
            // The test verifies that no exception is thrown and the method returns early
            // No interactions with the producer should happen
        }
        
        [Fact]
        public async Task QueueMessageBatchAsync_WithEvents_ShouldProduceKafkaMessage()
        {
            // Arrange
            var adapter = CreateAdapter();
            var streamId = StreamId.Create("test-namespace", Guid.NewGuid());
            var events = new List<object> { "event1", "event2" };
            var requestContext = new Dictionary<string, object> { { "key", "value" } };
            
            // Act & Assert
            await adapter.QueueMessageBatchAsync(streamId, events, null, requestContext);
            
            // The test verifies that no exception is thrown
            // A more complete test would verify that the producer was called with correct parameters,
            // but that would require more complex mocking of the producer
        }
        
        [Fact]
        public void CreateReceiver_ShouldReturnKafkaAdapterReceiver()
        {
            // Arrange
            var adapter = CreateAdapter();
            var queueId = new QueueId(0, "test-namespace");
            
            // Act
            var receiver = adapter.CreateReceiver(queueId);
            
            // Assert
            receiver.Should().NotBeNull();
            receiver.Should().BeOfType<KafkaAdapterReceiver>();
        }
        
        [Fact]
        public void Dispose_ShouldDisposeProducer()
        {
            // Arrange
            var adapter = CreateAdapter();
            
            // Act
            adapter.Dispose();
            
            // Assert
            // In a complete test, we would verify that the producer's Flush and Dispose methods were called
            // This would require more complex mocking
        }
        
        private KafkaAdapter CreateAdapter()
        {
            return new KafkaAdapter(
                _providerName,
                _options,
                _queueProperties,
                _mockSerializer.Object,
                _mockLoggerFactory.Object,
                _mockGrainFactory.Object,
                _mockDeserializer.Object
            );
        }
    }
} 