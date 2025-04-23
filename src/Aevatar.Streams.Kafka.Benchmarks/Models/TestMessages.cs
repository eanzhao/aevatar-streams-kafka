using System.Collections.Generic;

namespace Aevatar.Streams.Kafka.Benchmarks.Models
{
    // Simple message with few properties
    public class SimpleMessage
    {
        public string Id { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public long Timestamp { get; set; }
    }

    // Medium complexity message with more properties
    public class MediumMessage
    {
        public string Id { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public string Description { get; set; } = string.Empty;
        public long Timestamp { get; set; }
        public double Value { get; set; }
        public bool IsActive { get; set; }
        public string Category { get; set; } = string.Empty;
        public string Tags { get; set; } = string.Empty;
    }

    // Complex message with nested properties and collections
    public class ComplexMessage
    {
        public string Id { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public string Description { get; set; } = string.Empty;
        public long Timestamp { get; set; }
        public MetadataInfo Metadata { get; set; } = new MetadataInfo();
        public List<ItemInfo> Items { get; set; } = new List<ItemInfo>();
        public Dictionary<string, string> Properties { get; set; } = new Dictionary<string, string>();
    }

    public class MetadataInfo
    {
        public string Source { get; set; } = string.Empty;
        public string Version { get; set; } = string.Empty;
        public long CreatedAt { get; set; }
        public string CreatedBy { get; set; } = string.Empty;
        public LocationInfo Location { get; set; } = new LocationInfo();
    }

    public class LocationInfo
    {
        public double Latitude { get; set; }
        public double Longitude { get; set; }
        public string Region { get; set; } = string.Empty;
        public string Country { get; set; } = string.Empty;
    }

    public class ItemInfo
    {
        public string Id { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public double Quantity { get; set; }
        public double Price { get; set; }
        public List<string> Tags { get; set; } = new List<string>();
    }
} 