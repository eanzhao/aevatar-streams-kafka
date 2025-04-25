using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Orleans.Concurrency;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Config;
using Orleans.Streams.Kafka.Consumer;
using Orleans.Streams.Utils;
using Orleans.Streams.Utils.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using SerializationContext = Orleans.Streams.Kafka.Serialization.SerializationContext;
using SerializationContextMemoryPack = Orleans.Streams.Kafka.Serialization.SerializationContextMemoryPack;

namespace Orleans.Streams.Kafka.Core
{
	public class KafkaAdapterReceiver : IQueueAdapterReceiver
	{
		private readonly ILogger<KafkaAdapterReceiver> _logger;
		private readonly string _providerName;
		private readonly KafkaStreamOptions _options;
		private readonly OrleansMemoryPackSerializer _serializationManager;
		private readonly IGrainFactory _grainFactory;
		private readonly IExternalStreamDeserializer _externalDeserializer;
		private readonly QueueProperties _queueProperties;
		private readonly BatchMetrics _batchMetrics;
		private readonly object _batchSizeLock = new object();
		private int _currentBatchSize;
		private DateTime _lastBatchSizeAdjustment;

		private IConsumer<byte[], byte[]> _consumer;
		private Task _commitPromise = Task.CompletedTask;
		private Task<IList<IBatchContainer>> _consumePromise;

		public KafkaAdapterReceiver(
			string providerName,
			QueueProperties queueProperties,
			KafkaStreamOptions options,
			OrleansMemoryPackSerializer serializationManager,
			ILoggerFactory loggerFactory,
			IGrainFactory grainFactory,
			IExternalStreamDeserializer externalDeserializer
		)
		{
			_options = options ?? throw new ArgumentNullException(nameof(options));

			_providerName = providerName;
			_queueProperties = queueProperties;
			_serializationManager = serializationManager;
			_grainFactory = grainFactory;
			_externalDeserializer = externalDeserializer;
			_logger = loggerFactory.CreateLogger<KafkaAdapterReceiver>();
			_batchMetrics = new BatchMetrics(TimeSpan.FromMinutes(5));
			_currentBatchSize = _options.InitialBatchSize;
			_lastBatchSizeAdjustment = DateTime.UtcNow;
		}

		public Task Initialize(TimeSpan timeout)
		{
			_consumer = new ConsumerBuilder<byte[], byte[]>(_options.ToConsumerProperties())
				.SetErrorHandler((sender, errorEvent) =>
					_logger.LogError(
						"Consume error reason: {reason}, code: {code}, is broker error: {errorType}",
						errorEvent.Reason,
						errorEvent.Code,
						errorEvent.IsBrokerError
					))
				.Build();

			var offsetMode = Offset.Stored;
			switch (_options.ConsumeMode)
			{
				case ConsumeMode.LastCommittedMessage:
					offsetMode = Offset.Stored;
					break;
				case ConsumeMode.StreamEnd:
					offsetMode = Offset.End;
					break;
				case ConsumeMode.StreamStart:
					offsetMode = Offset.Beginning;
					break;
			}

			_consumer.Assign(new TopicPartitionOffset(_queueProperties.Namespace, (int)_queueProperties.PartitionId, offsetMode));

			return Task.CompletedTask;
		}

		public Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
		{
			var consumerRef = _consumer; // store direct ref, in case we are somehow asked to shutdown while we are receiving.

			if (consumerRef == null)
				return Task.FromResult<IList<IBatchContainer>>(new List<IBatchContainer>());

			var cancellationSource = new CancellationTokenSource();
			cancellationSource.CancelAfter(_options.PollBufferTimeout);

			_consumePromise = Task.Run(
				() => PollForMessages(
					maxCount,
					cancellationSource
				),
				cancellationSource.Token
			);

			return _consumePromise;
		}

		public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
		{
			KafkaBatchContainer batchWithHighestOffset = null;

			try
			{
				if (!messages.Any())
					return;

				batchWithHighestOffset = messages
					.Cast<KafkaBatchContainer>()
					.Max();

				_commitPromise = _consumer.Commit(batchWithHighestOffset);
				await _commitPromise;
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to commit message offset: {@offset}", batchWithHighestOffset?.TopicPartitionOffSet);
				throw;
			}
		}

		public async Task Shutdown(TimeSpan timeout)
		{
			try
			{
				var tasks = new List<Task>();

				if (_commitPromise != null)
					tasks.Add(_commitPromise);

				if (_consumePromise != null)
					tasks.Add(_consumePromise);

				await Task.WhenAll(tasks);
			}
			finally
			{
				_consumer.Unassign();
				_consumer.Unsubscribe();
				_consumer.Close();
				_consumer = null;
			}
		}

		private async Task<IList<IBatchContainer>> PollForMessages(int maxCount, CancellationTokenSource cancellation)
		{
			try
			{
				var startTime = DateTime.UtcNow;
				var batches = new List<IBatchContainer>();
				var targetBatchSize = GetTargetBatchSize();

				for (var i = 0; i < targetBatchSize && !cancellation.IsCancellationRequested; i++)
				{
					var consumeResult = _consumer.Consume(_options.PollTimeout);
					if (consumeResult == null)
						break;

					var batchContainer = consumeResult.ToBatchContainer(
						new SerializationContextMemoryPack
						{
							SerializationManager = _serializationManager,
							ExternalStreamDeserializer = _externalDeserializer
						},
						_queueProperties
					);

					await TrackMessage(batchContainer);

					batches.Add(batchContainer);
				}

				if (_options.EnableBatchMetrics)
				{
					var processingTime = DateTime.UtcNow - startTime;
					_batchMetrics.RecordBatch(_queueProperties.QueueName, batches.Count, processingTime);
				}

				AdjustBatchSize(batches.Count);

				return batches;
			}
			catch (OperationCanceledException ex) when (ex.CancellationToken.IsCancellationRequested)
			{
				return new List<IBatchContainer>();
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to poll for messages queueId: {@queueProperties}", _queueProperties);
				throw;
			}
			finally
			{
				cancellation.Dispose();
			}
		}

		private int GetTargetBatchSize()
		{
			if (!_options.EnableAdaptiveBatching)
			{
				return _options.MaxBatchSize;
			}

			lock (_batchSizeLock)
			{
				return _currentBatchSize;
			}
		}

		private void AdjustBatchSize(int actualBatchSize)
		{
			if (!_options.EnableAdaptiveBatching)
			{
				return;
			}

			lock (_batchSizeLock)
			{
				var now = DateTime.UtcNow;
				if (now - _lastBatchSizeAdjustment < _options.BatchSizeAdjustmentInterval)
				{
					return;
				}

				var metrics = _batchMetrics.GetMetrics(_queueProperties.QueueName);
				if (metrics.TotalBatches == 0)
				{
					return;
				}

				// 如果实际批处理大小接近目标大小，且处理时间在可接受范围内，则增加批处理大小
				if (actualBatchSize >= _currentBatchSize * 0.9 && 
					metrics.AverageProcessingTime.TotalMilliseconds < 100)
				{
					_currentBatchSize = Math.Min(
						(int)(_currentBatchSize * _options.BatchSizeAdjustmentFactor),
						_options.MaxBatchSize
					);
				}
				// 如果处理时间过长，则减少批处理大小
				else if (metrics.AverageProcessingTime.TotalMilliseconds > 200)
				{
					_currentBatchSize = Math.Max(
						(int)(_currentBatchSize / _options.BatchSizeAdjustmentFactor),
						_options.MinBatchSize
					);
				}

				_lastBatchSizeAdjustment = now;
				_logger.LogInformation(
					"Adjusted batch size for queue {queueName} to {batchSize}. Average processing time: {processingTime}ms",
					_queueProperties.QueueName,
					_currentBatchSize,
					metrics.AverageProcessingTime.TotalMilliseconds
				);
			}
		}

		private Task TrackMessage(IBatchContainer container)
		{
			if (!_options.MessageTrackingEnabled)
				return Task.CompletedTask;

			var trackingGrain = _grainFactory.GetMessageTrackerGrain(_providerName, _queueProperties.QueueName);
			return trackingGrain.Track(new Immutable<IBatchContainer>(container));
		}
	}
}