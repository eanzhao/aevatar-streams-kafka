using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Config;
using Orleans.Streams.Kafka.Core;
using Orleans.Streams.Utils;
using Orleans.Streams.Utils.Serialization;
using System;
using System.Threading.Tasks;
using Xunit;

namespace Aevatar.Streams.Kafka.Tests.Core
{
    public class KafkaAdapterReceiverTests : IClassFixture<TestFixture>
    {
        private readonly TestFixture _fixture;
        private readonly Mock<ILogger<KafkaAdapterReceiver>> _mockLogger;
        private readonly QueueProperties _queueProperties;

        public KafkaAdapterReceiverTests(TestFixture fixture)
        {
            _fixture = fixture;
            _mockLogger = new Mock<ILogger<KafkaAdapterReceiver>>();
            _fixture.MockLoggerFactory.Setup(x => x.CreateLogger<KafkaAdapterReceiver>()).Returns(_mockLogger.Object);
            _queueProperties = _fixture.QueueProperties["test-namespace"];
        }

        [Fact]
        public void Constructor_ShouldInitializeCorrectly()
        {
            // Arrange & Act
            var receiver = CreateReceiver();

            // Assert
            receiver.Should().NotBeNull();
        }

        [Fact]
        public async Task Initialize_ShouldNotThrowException()
        {
            // Arrange
            var receiver = CreateReceiver();

            // Act & Assert
            await receiver.Initialize(default);
            // The test verifies that no exception is thrown during initialization
        }

        [Fact]
        public async Task GetQueueMessagesAsync_ShouldReturnEmptyList_WhenNoMessagesAvailable()
        {
            // Arrange
            var receiver = CreateReceiver();
            await receiver.Initialize(default);

            // Act
            var result = await receiver.GetQueueMessagesAsync(100);

            // Assert
            result.Should().NotBeNull();
            result.Should().BeEmpty();
        }

        [Fact]
        public void Shutdown_ShouldNotThrowException()
        {
            // Arrange
            var receiver = CreateReceiver();

            // Act & Assert
            receiver.Shutdown(TimeSpan.FromSeconds(5));
            // The test verifies that no exception is thrown during shutdown
        }

        private KafkaAdapterReceiver CreateReceiver()
        {
            return new KafkaAdapterReceiver(
                _fixture.ProviderName,
                _queueProperties,
                _fixture.Options,
                _fixture.MockSerializer.Object,
                _fixture.MockLoggerFactory.Object,
                _fixture.MockGrainFactory.Object,
                _fixture.MockDeserializer.Object
            );
        }
    }
} 