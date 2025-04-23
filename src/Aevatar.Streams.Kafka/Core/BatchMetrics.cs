using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Orleans.Streams.Kafka.Core
{
	public class BatchMetrics
	{
		private readonly ConcurrentDictionary<string, QueueMetrics> _queueMetrics;
		private readonly TimeSpan _windowSize;
		private readonly int _maxSamples;

		public BatchMetrics(TimeSpan windowSize, int maxSamples = 100)
		{
			_windowSize = windowSize;
			_maxSamples = maxSamples;
			_queueMetrics = new ConcurrentDictionary<string, QueueMetrics>();
		}

		public void RecordBatch(string queueName, int batchSize, TimeSpan processingTime)
		{
			var metrics = _queueMetrics.GetOrAdd(queueName, _ => new QueueMetrics(_windowSize, _maxSamples));
			metrics.RecordBatch(batchSize, processingTime);
		}

		public BatchMetricsSnapshot GetMetrics(string queueName)
		{
			if (_queueMetrics.TryGetValue(queueName, out var metrics))
			{
				return metrics.GetSnapshot();
			}
			return new BatchMetricsSnapshot();
		}

		private class QueueMetrics
		{
			private readonly Queue<BatchSample> _samples;
			private readonly TimeSpan _windowSize;
			private readonly int _maxSamples;
			private DateTime _lastCleanup;

			public QueueMetrics(TimeSpan windowSize, int maxSamples)
			{
				_windowSize = windowSize;
				_maxSamples = maxSamples;
				_samples = new Queue<BatchSample>();
				_lastCleanup = DateTime.UtcNow;
			}

			public void RecordBatch(int batchSize, TimeSpan processingTime)
			{
				CleanupOldSamples();
				_samples.Enqueue(new BatchSample
				{
					Timestamp = DateTime.UtcNow,
					BatchSize = batchSize,
					ProcessingTime = processingTime
				});

				while (_samples.Count > _maxSamples)
				{
					_samples.Dequeue();
				}
			}

			public BatchMetricsSnapshot GetSnapshot()
			{
				CleanupOldSamples();
				if (_samples.Count == 0)
				{
					return new BatchMetricsSnapshot();
				}

				var recentSamples = _samples.ToList();
				return new BatchMetricsSnapshot
				{
					AverageBatchSize = recentSamples.Average(s => s.BatchSize),
					AverageProcessingTime = TimeSpan.FromTicks((long)recentSamples.Average(s => s.ProcessingTime.Ticks)),
					MaxBatchSize = recentSamples.Max(s => s.BatchSize),
					MinBatchSize = recentSamples.Min(s => s.BatchSize),
					TotalBatches = recentSamples.Count
				};
			}

			private void CleanupOldSamples()
			{
				var now = DateTime.UtcNow;
				if (now - _lastCleanup < TimeSpan.FromSeconds(1))
				{
					return;
				}

				var cutoff = now - _windowSize;
				while (_samples.Count > 0 && _samples.Peek().Timestamp < cutoff)
				{
					_samples.Dequeue();
				}

				_lastCleanup = now;
			}
		}

		private class BatchSample
		{
			public DateTime Timestamp { get; set; }
			public int BatchSize { get; set; }
			public TimeSpan ProcessingTime { get; set; }
		}
	}

	public class BatchMetricsSnapshot
	{
		public double AverageBatchSize { get; set; }
		public TimeSpan AverageProcessingTime { get; set; }
		public int MaxBatchSize { get; set; }
		public int MinBatchSize { get; set; }
		public int TotalBatches { get; set; }
	}
} 