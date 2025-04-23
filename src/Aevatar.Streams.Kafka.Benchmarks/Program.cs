using BenchmarkDotNet.Running;
using Aevatar.Streams.Kafka.Benchmarks;

// Run the benchmarks
BenchmarkRunner.Run<JsonDeserializerBenchmark>(); 