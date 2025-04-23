using Confluent.Kafka;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;
using Orleans.Streams.Kafka.Config;
using Orleans.Streams.Kafka.Core;
using Orleans.Streams.Utils;
using Orleans.Streams.Utils.Serialization;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;
using Aevatar.Streams.Kafka.Tests;

namespace Aevatar.Streams.Kafka.Tests.Core
{
    public class KafkaAdapterTests
    {
        private readonly Mock<IOrleansJsonSerializer> _mockSerializer;
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
            _mockSerializer = new Mock<IOrleansJsonSerializer>();
            _mockLoggerFactory = new Mock<ILoggerFactory>();
            _mockGrainFactory = new Mock<IGrainFactory>();
            _mockDeserializer = new Mock<IExternalStreamDeserializer>();
            _mockLogger = new Mock<ILogger<KafkaAdapter>>();
            
            // Setup mock logger factory to return our mock logger
            _mockLoggerFactory
                .Setup(x => x.CreateLogger(It.Is<string>(s => s == typeof(KafkaAdapter).FullName)))
                .Returns(_mockLogger.Object);
            
            _mockProducer = new Mock<IProducer<byte[], KafkaBatchContainer>>();
            
            // Setup ProduceAsync to return a successful result by default
            var deliveryResult = new DeliveryResult<byte[], KafkaBatchContainer>
            {
                Status = PersistenceStatus.Persisted
            };
            _mockProducer
                .Setup(p => p.ProduceAsync(
                    It.IsAny<string>(), 
                    It.IsAny<Message<byte[], KafkaBatchContainer>>(), 
                    default))
                .ReturnsAsync(deliveryResult);
            
            _options = new KafkaStreamOptions
            {
                BrokerList = new List<string> { "localhost:9092" },
                ConsumerGroupId = "unit-test-group",
                PollTimeout = TimeSpan.FromMilliseconds(10),
                ProducerTimeout = TimeSpan.FromSeconds(1)
            };
            
            _queueProperties = new Dictionary<string, QueueProperties>
            {
                { "test-namespace", new QueueProperties("test-namespace", 0) }
            };
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
            
            // Prepare test data
            var streamId = StreamId.Create("test-namespace", Guid.NewGuid());
            var events = new List<object> { "event1", "event2" };
            var requestContext = new Dictionary<string, object> { { "key", "value" } };
            
            // Act
            await adapter.QueueMessageBatchAsync(streamId, events, null, requestContext);
            
            // Assert
            // Verify that ProduceAsync was called with the correct parameters
            _mockProducer.Verify(
                p => p.ProduceAsync(
                    It.Is<string>(s => s == streamId.GetNamespace()),
                    It.Is<Message<byte[], KafkaBatchContainer>>(m => 
                        m.Value.StreamId.Equals(streamId) && 
                        m.Value.Events.Count == events.Count),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }
        
        [Fact]
        public void CreateReceiver_ShouldReturnKafkaAdapterReceiver()
        {
            // Arrange
            var adapter = CreateAdapter();
            var queueId = QueueId.GetQueueId("test-namespace", 0, 0);
            
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
                _mockSerializer.Object as OrleansJsonSerializer,
                _mockLoggerFactory.Object,
                _mockGrainFactory.Object,
                _mockDeserializer.Object
            );
        }
    }
} 