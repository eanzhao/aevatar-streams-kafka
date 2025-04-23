using Aevatar.Streams.Kafka.Benchmarks.Models;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Orleans.Streams.Utils;
using Orleans.Streams.Utils.Serialization;
using System;
using System.Threading.Tasks;
using System.Text.Json;

namespace Aevatar.Streams.Kafka.Benchmarks
{
    [MemoryDiagnoser]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    [RankColumn]
    public class JsonDeserializerBenchmark
    {
        private TestJsonDataGenerator _dataGenerator;
        private JsonExternalStreamDeserializer _jsonDeserializer;
        private QueueProperties _queueProperties;
        
        // Test data
        private byte[] _simpleMessageData;
        private byte[] _mediumMessageData;
        private byte[] _complexMessageData;

        [GlobalSetup]
        public void Setup()
        {
            // Initialize data generator
            _dataGenerator = new TestJsonDataGenerator();
            
            // Initialize deserializer
            _jsonDeserializer = new JsonExternalStreamDeserializer();
            
            // Setup queue properties
            _queueProperties = new QueueProperties("test-topic");
            
            // Generate test data
            _simpleMessageData = _dataGenerator.GenerateSimpleMessageData();
            _mediumMessageData = _dataGenerator.GenerateMediumMessageData();
            _complexMessageData = _dataGenerator.GenerateComplexMessageData();
            
            // Warm up
            _jsonDeserializer.Deserialize(_queueProperties, typeof(SimpleMessage), _simpleMessageData);
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _jsonDeserializer.Dispose();
        }

        [Benchmark(Baseline = true)]
        public object DeserializeSimpleMessageJson()
        {
            return _jsonDeserializer.Deserialize(_queueProperties, typeof(SimpleMessage), _simpleMessageData);
        }

        [Benchmark]
        public object DeserializeMediumMessageJson()
        {
            return _jsonDeserializer.Deserialize(_queueProperties, typeof(MediumMessage), _mediumMessageData);
        }

        [Benchmark]
        public object DeserializeComplexMessageJson()
        {
            return _jsonDeserializer.Deserialize(_queueProperties, typeof(ComplexMessage), _complexMessageData);
        }
    }

    // Helper class to generate JSON test data
    public class TestJsonDataGenerator
    {
        public byte[] GenerateSimpleMessageData()
        {
            var message = new SimpleMessage
            {
                Id = Guid.NewGuid().ToString(),
                Name = "Simple Test Message",
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            };
            
            return JsonSerializer.SerializeToUtf8Bytes(message);
        }

        public byte[] GenerateMediumMessageData()
        {
            var message = new MediumMessage
            {
                Id = Guid.NewGuid().ToString(),
                Name = "Medium Test Message",
                Description = "This is a test message with medium complexity for benchmarking",
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Value = 25.6,
                IsActive = true,
                Category = "Test",
                Tags = "benchmark,test,medium"
            };
            
            return JsonSerializer.SerializeToUtf8Bytes(message);
        }

        public byte[] GenerateComplexMessageData()
        {
            var message = new ComplexMessage
            {
                Id = Guid.NewGuid().ToString(),
                Name = "Complex Test Message",
                Description = "This is a complex test message with nested objects and collections for benchmarking",
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Metadata = new MetadataInfo
                {
                    Source = "Benchmark Test",
                    Version = "1.0.0",
                    CreatedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    CreatedBy = "TestDataGenerator",
                    Location = new LocationInfo
                    {
                        Latitude = 37.7749,
                        Longitude = -122.4194,
                        Region = "California",
                        Country = "USA"
                    }
                },
                Items = new System.Collections.Generic.List<ItemInfo>
                {
                    new ItemInfo
                    {
                        Id = "item1",
                        Name = "Test Item 1",
                        Quantity = 10,
                        Price = 9.99,
                        Tags = new System.Collections.Generic.List<string> { "tag1", "tag2" }
                    },
                    new ItemInfo
                    {
                        Id = "item2",
                        Name = "Test Item 2",
                        Quantity = 5,
                        Price = 19.99,
                        Tags = new System.Collections.Generic.List<string> { "tag3", "tag4" }
                    }
                },
                Properties = new System.Collections.Generic.Dictionary<string, string>
                {
                    { "prop1", "value1" },
                    { "prop2", "value2" },
                    { "prop3", "value3" }
                }
            };
            
            return JsonSerializer.SerializeToUtf8Bytes(message);
        }
    }
} 