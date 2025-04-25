using Aevatar.Streams.Kafka.Config;
using Aevatar.Streams.Kafka.Producer;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Aevatar.Streams.Kafka.Tests.Producer
{
    public class KafkaBatchProducerTests
    {
        private readonly Mock<ILogger<KafkaBatchProducer<string, string>>> _loggerMock;
        private readonly KafkaBatchProducerConfig _config;

        public KafkaBatchProducerTests()
        {
            _loggerMock = new Mock<ILogger<KafkaBatchProducer<string, string>>>();
            _config = new KafkaBatchProducerConfig
            {
                BootstrapServers = "localhost:9092",
                ClientId = "test-client",
                RetryCount = 3,
                RetryInterval = TimeSpan.FromMilliseconds(100)
            };
        }

        [Fact]
        public void Constructor_WithValidConfig_InitializesProducer()
        {
            // Arrange & Act
            var producer = new TestKafkaBatchProducer(_config, _loggerMock.Object);

            // Assert
            Assert.NotNull(producer.Producer);
            Assert.Equal(_config, producer.Config);
        }

        [Fact]
        public void Constructor_WithNullConfig_ThrowsArgumentNullException()
        {
            // Arrange & Act & Assert
            Assert.Throws<ArgumentNullException>(() => new KafkaBatchProducer<string, string>(null, _loggerMock.Object));
        }

        [Fact]
        public void Constructor_WithNullLogger_ThrowsArgumentNullException()
        {
            // Arrange & Act & Assert
            Assert.Throws<ArgumentNullException>(() => new KafkaBatchProducer<string, string>(_config, null));
        }

        [Fact]
        public async Task ProduceAsync_WithValidParameters_CallsProducerProduceAsync()
        {
            // Arrange
            var topic = "test-topic";
            var key = "test-key";
            var value = "test-value";
            var headers = new Dictionary<string, object> { { "header1", "value1" } };

            var deliveryResult = new DeliveryResult<string, string>
            {
                Topic = topic,
                Partition = new Partition(0),
                Offset = new Offset(1)
            };

            var mockProducer = new Mock<IProducer<string, string>>();
            mockProducer
                .Setup(p => p.ProduceAsync(
                    It.IsAny<string>(),
                    It.IsAny<Message<string, string>>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(deliveryResult);

            var producer = new TestKafkaBatchProducer(_config, _loggerMock.Object, mockProducer.Object);

            // Act
            await producer.ProduceAsync(topic, key, value, headers);

            // Assert
            mockProducer.Verify(
                p => p.ProduceAsync(
                    topic,
                    It.Is<Message<string, string>>(m => 
                        m.Key == key && 
                        m.Value == value && 
                        m.Headers != null),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task ProduceAsync_WhenTransientErrorOccurs_RetriesSpecifiedNumberOfTimes()
        {
            // Arrange
            var topic = "test-topic";
            var key = "test-key";
            var value = "test-value";
            var deliveryResult = new DeliveryResult<string, string>
            {
                Topic = topic,
                Partition = new Partition(0),
                Offset = new Offset(1)
            };

            var mockProducer = new Mock<IProducer<string, string>>();
            var callCount = 0;
            mockProducer
                .Setup(p => p.ProduceAsync(
                    It.IsAny<string>(),
                    It.IsAny<Message<string, string>>(),
                    It.IsAny<CancellationToken>()))
                .Returns(() =>
                {
                    callCount++;
                    if (callCount <= 2)
                    {
                        throw new ProduceException<string, string>(
                            new Error(Confluent.Kafka.ErrorCode.NetworkException, "Test network error", true), 
                            deliveryResult);
                    }
                    return Task.FromResult(deliveryResult);
                });

            var producer = new TestKafkaBatchProducer(_config, _loggerMock.Object, mockProducer.Object);

            // Act
            await producer.ProduceAsync(topic, key, value);

            // Assert
            Assert.Equal(3, callCount);  // Initial attempt + 2 retries
            mockProducer.Verify(
                p => p.ProduceAsync(
                    It.IsAny<string>(),
                    It.IsAny<Message<string, string>>(),
                    It.IsAny<CancellationToken>()),
                Times.Exactly(3));
        }

        [Fact]
        public async Task ProduceBatchAsync_WithValidParameters_CallsProduceAsyncForEachMessage()
        {
            // Arrange
            var topic = "test-topic";
            var messages = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>("key1", "value1"),
                new KeyValuePair<string, string>("key2", "value2"),
                new KeyValuePair<string, string>("key3", "value3")
            };

            var mockProducer = new Mock<IProducer<string, string>>();
            mockProducer
                .Setup(p => p.ProduceAsync(
                    It.IsAny<string>(),
                    It.IsAny<Message<string, string>>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new DeliveryResult<string, string>());

            var producer = new TestKafkaBatchProducer(_config, _loggerMock.Object, mockProducer.Object);

            // Act
            await producer.ProduceBatchAsync(topic, messages);

            // Assert
            mockProducer.Verify(
                p => p.ProduceAsync(
                    It.IsAny<string>(),
                    It.IsAny<Message<string, string>>(),
                    It.IsAny<CancellationToken>()),
                Times.Exactly(messages.Count));
        }

        [Fact]
        public async Task ProduceBatchAsync_WithNullMessages_ThrowsArgumentNullException()
        {
            // Arrange
            var producer = new TestKafkaBatchProducer(_config, _loggerMock.Object);

            // Act & Assert
            await Assert.ThrowsAsync<ArgumentNullException>(() => 
                producer.ProduceBatchAsync("test-topic", null));
        }

        [Fact]
        public async Task ProduceBatchAsync_WithEmptyMessages_ReturnsWithoutCallingProduceAsync()
        {
            // Arrange
            var mockProducer = new Mock<IProducer<string, string>>();
            var producer = new TestKafkaBatchProducer(_config, _loggerMock.Object, mockProducer.Object);

            // Act
            await producer.ProduceBatchAsync("test-topic", new List<KeyValuePair<string, string>>());

            // Assert
            mockProducer.Verify(
                p => p.ProduceAsync(
                    It.IsAny<string>(),
                    It.IsAny<Message<string, string>>(),
                    It.IsAny<CancellationToken>()),
                Times.Never);
        }

        [Fact]
        public async Task ProduceAndWaitAsync_WithValidParameters_ReturnsDeliveryResult()
        {
            // Arrange
            var topic = "test-topic";
            var key = "test-key";
            var value = "test-value";
            var expectedResult = new DeliveryResult<string, string>
            {
                Topic = topic,
                Partition = new Partition(0),
                Offset = new Offset(1)
            };

            var mockProducer = new Mock<IProducer<string, string>>();
            mockProducer
                .Setup(p => p.ProduceAsync(
                    It.IsAny<string>(),
                    It.IsAny<Message<string, string>>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(expectedResult);

            var producer = new TestKafkaBatchProducer(_config, _loggerMock.Object, mockProducer.Object);

            // Act
            var result = await producer.ProduceAndWaitAsync(topic, key, value);

            // Assert
            Assert.Equal(expectedResult, result);
        }

        [Fact]
        public void Dispose_CallsProducerDispose()
        {
            // Arrange
            var mockProducer = new Mock<IProducer<string, string>>();
            var producer = new TestKafkaBatchProducer(_config, _loggerMock.Object, mockProducer.Object);

            // Act
            producer.Dispose();

            // Assert
            mockProducer.Verify(p => p.Dispose(), Times.Once);
            Assert.True(producer.IsDisposed);
        }

        [Fact]
        public void Dispose_CalledMultipleTimes_OnlyDisposesProducerOnce()
        {
            // Arrange
            var mockProducer = new Mock<IProducer<string, string>>();
            var producer = new TestKafkaBatchProducer(_config, _loggerMock.Object, mockProducer.Object);

            // Act
            producer.Dispose();
            producer.Dispose();

            // Assert
            mockProducer.Verify(p => p.Dispose(), Times.Once);
        }

        // Helper class to expose internal properties and methods for testing
        private class TestKafkaBatchProducer : KafkaBatchProducer<string, string>
        {
            public IProducer<string, string> Producer => _producer;
            public KafkaBatchProducerConfig Config => _config;
            public bool IsDisposed => _disposed;

            public TestKafkaBatchProducer(
                KafkaBatchProducerConfig config, 
                ILogger<KafkaBatchProducer<string, string>> logger,
                IProducer<string, string> producerOverride = null) 
                : base(config, logger)
            {
                if (producerOverride != null)
                {
                    _producer = producerOverride;
                }
            }

            protected override void InitializeProducer()
            {
                if (_producer == null)
                {
                    base.InitializeProducer();
                }
            }
        }
    }
} 