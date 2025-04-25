using Aevatar.Streams.Kafka.Config;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Aevatar.Streams.Kafka.Producer
{
    /// <summary>
    /// Kafka batch producer for efficiently sending messages to Kafka topics.
    /// </summary>
    /// <typeparam name="TKey">The type of message key.</typeparam>
    /// <typeparam name="TValue">The type of message value.</typeparam>
    public class KafkaBatchProducer<TKey, TValue> : IDisposable
    {
        protected readonly KafkaBatchProducerConfig _config;
        protected readonly ILogger<KafkaBatchProducer<TKey, TValue>> _logger;
        protected IProducer<TKey, TValue> _producer;
        protected bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="KafkaBatchProducer{TKey, TValue}"/> class.
        /// </summary>
        /// <param name="config">The producer configuration.</param>
        /// <param name="logger">The logger.</param>
        /// <exception cref="ArgumentNullException">Thrown when config or logger is null.</exception>
        public KafkaBatchProducer(KafkaBatchProducerConfig config, ILogger<KafkaBatchProducer<TKey, TValue>> logger)
        {
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            
            InitializeProducer();
        }

        /// <summary>
        /// Initializes the Kafka producer.
        /// </summary>
        protected virtual void InitializeProducer()
        {
            _logger.LogInformation("Initializing Kafka producer with bootstrap servers: {BootstrapServers}", _config.BootstrapServers);
            try
            {
                var producerConfig = _config.BuildProducerConfig();
                _producer = new ProducerBuilder<TKey, TValue>(producerConfig).Build();
                _logger.LogInformation("Kafka producer initialized successfully");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize Kafka producer");
                throw;
            }
        }

        /// <summary>
        /// Produces a single message to the specified topic.
        /// </summary>
        /// <param name="topic">The topic to produce to.</param>
        /// <param name="key">The message key.</param>
        /// <param name="value">The message value.</param>
        /// <param name="headers">Optional message headers.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task that completes when the message is delivered to Kafka.</returns>
        public virtual async Task ProduceAsync(
            string topic,
            TKey key,
            TValue value,
            IDictionary<string, object> headers = null,
            CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(topic))
                throw new ArgumentNullException(nameof(topic));

            var message = new Message<TKey, TValue>
            {
                Key = key,
                Value = value
            };

            if (headers != null && headers.Count > 0)
            {
                message.Headers = new Headers();
                foreach (var header in headers)
                {
                    if (header.Value != null)
                    {
                        AddHeaderValue(message.Headers, header.Key, header.Value);
                    }
                }
            }

            int attempt = 0;
            bool success = false;

            while (!success && attempt <= _config.RetryCount)
            {
                try
                {
                    attempt++;
                    _logger.LogDebug("Producing message to topic {Topic}, attempt {Attempt}/{MaxAttempts}", 
                        topic, attempt, _config.RetryCount + 1);
                    
                    await _producer.ProduceAsync(topic, message, cancellationToken).ConfigureAwait(false);
                    success = true;
                    
                    _logger.LogDebug("Successfully produced message to topic {Topic}", topic);
                }
                catch (ProduceException<TKey, TValue> ex) when (IsRetriableError(ex.Error) && attempt <= _config.RetryCount)
                {
                    _logger.LogWarning(ex, "Retriable error occurred while producing message to topic {Topic}. " +
                        "Retrying in {RetryInterval}ms. Attempt {Attempt}/{MaxAttempts}",
                        topic, _config.RetryInterval.TotalMilliseconds, attempt, _config.RetryCount + 1);
                    
                    await Task.Delay(_config.RetryInterval, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to produce message to topic {Topic} after {Attempt} attempts", 
                        topic, attempt);
                    throw;
                }
            }
        }

        /// <summary>
        /// Produces a batch of messages to the specified topic.
        /// </summary>
        /// <param name="topic">The topic to produce to.</param>
        /// <param name="messages">The collection of key-value pairs to produce.</param>
        /// <param name="headers">Optional message headers to apply to all messages.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task that completes when all messages are delivered to Kafka.</returns>
        public virtual async Task ProduceBatchAsync(
            string topic,
            ICollection<KeyValuePair<TKey, TValue>> messages,
            IDictionary<string, object> headers = null,
            CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(topic))
                throw new ArgumentNullException(nameof(topic));
                
            if (messages == null)
                throw new ArgumentNullException(nameof(messages));
                
            if (messages.Count == 0)
                return;

            _logger.LogInformation("Producing batch of {Count} messages to topic {Topic}", 
                messages.Count, topic);

            var tasks = messages.Select(message => 
                ProduceAsync(topic, message.Key, message.Value, headers, cancellationToken)).ToList();

            await Task.WhenAll(tasks).ConfigureAwait(false);
            
            _logger.LogInformation("Successfully produced batch of {Count} messages to topic {Topic}", 
                messages.Count, topic);
        }

        /// <summary>
        /// Produces a message and waits for delivery confirmation.
        /// </summary>
        /// <param name="topic">The topic to produce to.</param>
        /// <param name="key">The message key.</param>
        /// <param name="value">The message value.</param>
        /// <param name="headers">Optional message headers.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>The delivery result, containing information about the delivered message.</returns>
        public virtual async Task<DeliveryResult<TKey, TValue>> ProduceAndWaitAsync(
            string topic,
            TKey key,
            TValue value,
            IDictionary<string, object> headers = null,
            CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(topic))
                throw new ArgumentNullException(nameof(topic));

            var message = new Message<TKey, TValue>
            {
                Key = key,
                Value = value
            };

            if (headers != null && headers.Count > 0)
            {
                message.Headers = new Headers();
                foreach (var header in headers)
                {
                    if (header.Value != null)
                    {
                        AddHeaderValue(message.Headers, header.Key, header.Value);
                    }
                }
            }

            int attempt = 0;
            DeliveryResult<TKey, TValue> result = null;

            while (result == null && attempt <= _config.RetryCount)
            {
                try
                {
                    attempt++;
                    _logger.LogDebug("Producing message to topic {Topic} and waiting for confirmation, attempt {Attempt}/{MaxAttempts}", 
                        topic, attempt, _config.RetryCount + 1);
                    
                    result = await _producer.ProduceAsync(topic, message, cancellationToken).ConfigureAwait(false);
                    
                    _logger.LogDebug("Successfully produced message to topic {Topic}, partition {Partition}, offset {Offset}", 
                        result.Topic, result.Partition, result.Offset);
                }
                catch (ProduceException<TKey, TValue> ex) when (IsRetriableError(ex.Error) && attempt <= _config.RetryCount)
                {
                    _logger.LogWarning(ex, "Retriable error occurred while producing message to topic {Topic}. " +
                        "Retrying in {RetryInterval}ms. Attempt {Attempt}/{MaxAttempts}",
                        topic, _config.RetryInterval.TotalMilliseconds, attempt, _config.RetryCount + 1);
                    
                    await Task.Delay(_config.RetryInterval, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to produce message to topic {Topic} after {Attempt} attempts", 
                        topic, attempt);
                    throw;
                }
            }

            return result;
        }

        /// <summary>
        /// Adds a header value to the message headers.
        /// </summary>
        /// <param name="headers">The headers collection.</param>
        /// <param name="key">The header key.</param>
        /// <param name="value">The header value.</param>
        protected virtual void AddHeaderValue(Headers headers, string key, object value)
        {
            switch (value)
            {
                case byte[] byteArray:
                    headers.Add(key, byteArray);
                    break;
                case string str:
                    headers.Add(key, System.Text.Encoding.UTF8.GetBytes(str));
                    break;
                case int intValue:
                    headers.Add(key, BitConverter.GetBytes(intValue));
                    break;
                case long longValue:
                    headers.Add(key, BitConverter.GetBytes(longValue));
                    break;
                case double doubleValue:
                    headers.Add(key, BitConverter.GetBytes(doubleValue));
                    break;
                case bool boolValue:
                    headers.Add(key, BitConverter.GetBytes(boolValue));
                    break;
                case Guid guidValue:
                    headers.Add(key, guidValue.ToByteArray());
                    break;
                default:
                    var json = System.Text.Json.JsonSerializer.Serialize(value);
                    headers.Add(key, System.Text.Encoding.UTF8.GetBytes(json));
                    break;
            }
        }

        /// <summary>
        /// Determines if an error is retriable.
        /// </summary>
        /// <param name="error">The Kafka error.</param>
        /// <returns>True if the error is retriable; otherwise, false.</returns>
        protected virtual bool IsRetriableError(Error error)
        {
            return error.IsLocalError || 
                   error.IsBrokerError || 
                   error.Code == Confluent.Kafka.ErrorCode.NetworkException;
        }

        /// <summary>
        /// Disposes the producer instance.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Disposes resources used by the producer.
        /// </summary>
        /// <param name="disposing">Indicates whether the call comes from a Dispose method (true) or from a finalizer (false).</param>
        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            if (disposing)
            {
                _logger.LogInformation("Disposing Kafka producer");
                _producer?.Dispose();
            }

            _disposed = true;
        }
    }
} 