using Confluent.Kafka;
using System;
using System.Collections.Generic;

namespace Aevatar.Streams.Kafka.Config
{
    /// <summary>
    /// Configuration for Kafka batch producer.
    /// </summary>
    public class KafkaBatchProducerConfig
    {
        /// <summary>
        /// Gets or sets the Kafka bootstrap servers.
        /// </summary>
        public string BootstrapServers { get; set; }

        /// <summary>
        /// Gets or sets the number of retries for failed messages.
        /// </summary>
        public int RetryCount { get; set; } = 3;

        /// <summary>
        /// Gets or sets the retry interval in milliseconds.
        /// </summary>
        public TimeSpan RetryInterval { get; set; } = TimeSpan.FromSeconds(1);

        /// <summary>
        /// Gets or sets the Acks configuration.
        /// </summary>
        public Acks Acks { get; set; } = Acks.All;

        /// <summary>
        /// Gets or sets the message timeout in milliseconds.
        /// </summary>
        public TimeSpan MessageTimeout { get; set; } = TimeSpan.FromSeconds(10);

        /// <summary>
        /// Gets or sets the linger time in milliseconds.
        /// </summary>
        public TimeSpan LingerMs { get; set; } = TimeSpan.FromMilliseconds(5);

        /// <summary>
        /// Gets or sets the batch size in bytes.
        /// </summary>
        public int BatchSize { get; set; } = 16384;

        /// <summary>
        /// Gets or sets the compression type.
        /// </summary>
        public CompressionType CompressionType { get; set; } = CompressionType.Snappy;

        /// <summary>
        /// Gets or sets the client id.
        /// </summary>
        public string ClientId { get; set; }

        /// <summary>
        /// Gets or sets additional configuration parameters.
        /// </summary>
        public Dictionary<string, string> AdditionalConfig { get; set; } = new Dictionary<string, string>();

        /// <summary>
        /// Builds a ProducerConfig from this configuration.
        /// </summary>
        /// <returns>A producer configuration object.</returns>
        public ProducerConfig BuildProducerConfig()
        {
            if (string.IsNullOrWhiteSpace(BootstrapServers))
                throw new InvalidOperationException("BootstrapServers must be specified");

            var config = new ProducerConfig
            {
                BootstrapServers = BootstrapServers,
                Acks = Acks,
                MessageTimeoutMs = (int)MessageTimeout.TotalMilliseconds,
                LingerMs = (int)LingerMs.TotalMilliseconds,
                BatchSize = BatchSize,
                CompressionType = CompressionType,
                EnableIdempotence = Acks == Acks.All, // Enable idempotence when using Acks.All
                RetryBackoffMs = (int)RetryInterval.TotalMilliseconds
            };

            if (!string.IsNullOrWhiteSpace(ClientId))
            {
                config.ClientId = ClientId;
            }

            // Apply any additional configuration
            if (AdditionalConfig != null)
            {
                foreach (var kvp in AdditionalConfig)
                {
                    config.Set(kvp.Key, kvp.Value);
                }
            }

            return config;
        }
    }
} 