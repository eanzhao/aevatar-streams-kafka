# Aevatar.Streams.Kafka.Benchmarks

This project provides benchmarks for the `AvroExternalStreamDeserializer` class and compares its performance with `JsonExternalStreamDeserializer`.

## Overview

The benchmark project measures the deserialization performance of Avro and JSON serializers for different message types:
- Simple messages (few properties)
- Medium complexity messages (more properties)
- Complex messages (nested objects and collections)

## Running the Benchmarks

To run the benchmarks, execute the following command from the project directory:

```bash
dotnet run -c Release
```

## Benchmark Design

The benchmarks use BenchmarkDotNet to provide reliable measurements with the following features:
- Memory diagnostics to measure memory allocations
- Comparison between Avro and JSON deserialization
- Ordering results from fastest to slowest
- Various message complexities to test different scenarios

## Components

- **AvroDeserializerBenchmark.cs**: Main benchmark class with test methods
- **TestDataGenerator.cs**: Helper for generating test data
- **MockSchemaRegistryClient.cs**: Mock implementation of Schema Registry for testing
- **Models/TestMessages.cs**: Test message classes of varying complexity

## Sample Results

When you run the benchmarks, you'll see results similar to:

```
BenchmarkDotNet=v0.13.12
// ... benchmark configuration details ...

|                         Method |      Mean |     Error |    StdDev | Rank |     Gen0 |    Gen1 | Allocated |
|------------------------------- |----------:|----------:|----------:|-----:|---------:|--------:|----------:|
|          DeserializeSimpleMessage |  XX.XX μs |  X.XX μs |  X.XX μs |    1 |   X.XXXX |  X.XXXX |     XX KB |
|         DeserializeMediumMessage |  XX.XX μs |  X.XX μs |  X.XX μs |    2 |   X.XXXX |  X.XXXX |     XX KB |
|        DeserializeComplexMessage | XXX.XX μs | XX.XX μs | XX.XX μs |    3 |  XX.XXXX | XX.XXXX |    XXX KB |
|     DeserializeSimpleMessageJson |  XX.XX μs |  X.XX μs |  X.XX μs |    4 |   X.XXXX |  X.XXXX |     XX KB |
|    DeserializeMediumMessageJson |  XX.XX μs |  X.XX μs |  X.XX μs |    5 |   X.XXXX |  X.XXXX |     XX KB |
|   DeserializeComplexMessageJson | XXX.XX μs | XX.XX μs | XX.XX μs |    6 |  XX.XXXX | XX.XXXX |    XXX KB |
```

## Notes

- The benchmarks use a mock Schema Registry client for Avro serialization
- The JSON benchmarks first deserialize with Avro, then serialize to JSON, and finally deserialize with JSON serializer to enable fair comparison
- Memory allocation is measured to understand the memory footprint of each serializer 