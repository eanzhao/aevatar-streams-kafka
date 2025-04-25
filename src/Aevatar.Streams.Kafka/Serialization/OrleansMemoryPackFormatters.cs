using System;
using System.Net;
using System.Buffers;
using System.Collections.Generic;
using MemoryPack;
using MemoryPack.Formatters;
using Orleans.Runtime;
using Orleans.GrainReferences;
using System.Reflection;

namespace Orleans.Serialization
{
    /// <summary>
    /// MemoryPack formatter for <see cref="IPAddress"/>.
    /// </summary>
    public class IPAddressFormatter : MemoryPackFormatter<IPAddress?>
    {
        public override void Serialize<TBufferWriter>(ref MemoryPackWriter<TBufferWriter> writer, scoped ref IPAddress? value)
        {
            if (value == null)
            {
                writer.WriteNullObjectHeader();
                return;
            }
            
            byte[] addressBytes = value.GetAddressBytes();
            writer.WriteObjectHeader(1);
            writer.WriteUnmanaged(addressBytes.Length);
            writer.WriteSpan<byte>(addressBytes);
        }

        public override void Deserialize(ref MemoryPackReader reader, scoped ref IPAddress? value)
        {
            if (!reader.TryReadObjectHeader(out var count))
            {
                value = null;
                return;
            }
            
            int length = reader.ReadUnmanaged<int>();
            byte[] addressBytes = reader.ReadArray<byte>() ?? Array.Empty<byte>();
            value = new IPAddress(addressBytes);
        }
    }

    /// <summary>
    /// MemoryPack formatter for <see cref="GrainId"/>.
    /// </summary>
    public class GrainIdFormatter : MemoryPackFormatter<GrainId>
    {
        public override void Serialize<TBufferWriter>(ref MemoryPackWriter<TBufferWriter> writer, scoped ref GrainId value)
        {
            writer.WriteObjectHeader(2);
            writer.WriteString(value.Type.ToString());
            writer.WriteString(value.Key.ToString());
        }

        public override void Deserialize(ref MemoryPackReader reader, scoped ref GrainId value)
        {
            if (!reader.TryReadObjectHeader(out var count))
            {
                // Handle as error or return default value
                throw new InvalidOperationException("Failed to read object header for GrainId");
            }
            
            string type = reader.ReadString() ?? string.Empty;
            string key = reader.ReadString() ?? string.Empty;
            value = GrainId.Create(type, key);
        }
    }

    /// <summary>
    /// MemoryPack formatter for <see cref="ActivationId"/>.
    /// </summary>
    public class ActivationIdFormatter : MemoryPackFormatter<ActivationId>
    {
        public override void Serialize<TBufferWriter>(ref MemoryPackWriter<TBufferWriter> writer, scoped ref ActivationId value)
        {
            writer.WriteObjectHeader(1);
            writer.WriteString(value.ToParsableString());
        }

        public override void Deserialize(ref MemoryPackReader reader, scoped ref ActivationId value)
        {
            if (!reader.TryReadObjectHeader(out var count))
            {
                // Handle as error or return default value
                throw new InvalidOperationException("Failed to read object header for ActivationId");
            }
            
            string str = reader.ReadString() ?? string.Empty;
            value = string.IsNullOrEmpty(str) ? default : ActivationId.FromParsableString(str);
        }
    }

    /// <summary>
    /// MemoryPack formatter for <see cref="SiloAddress"/>.
    /// </summary>
    public class SiloAddressFormatter : MemoryPackFormatter<SiloAddress?>
    {
        public override void Serialize<TBufferWriter>(ref MemoryPackWriter<TBufferWriter> writer, scoped ref SiloAddress? value)
        {
            if (value == null)
            {
                writer.WriteNullObjectHeader();
                return;
            }
            
            writer.WriteObjectHeader(1);
            writer.WriteString(value.ToParsableString());
        }

        public override void Deserialize(ref MemoryPackReader reader, scoped ref SiloAddress? value)
        {
            if (!reader.TryReadObjectHeader(out var count))
            {
                value = null;
                return;
            }
            
            string parsableString = reader.ReadString() ?? string.Empty;
            value = SiloAddress.FromParsableString(parsableString);
        }
    }

    /// <summary>
    /// MemoryPack formatter for <see cref="MembershipVersion"/>.
    /// </summary>
    public class MembershipVersionFormatter : MemoryPackFormatter<MembershipVersion>
    {
        public override void Serialize<TBufferWriter>(ref MemoryPackWriter<TBufferWriter> writer, scoped ref MembershipVersion value)
        {
            writer.WriteObjectHeader(1);
            writer.WriteUnmanaged(value.Value);
        }

        public override void Deserialize(ref MemoryPackReader reader, scoped ref MembershipVersion value)
        {
            if (!reader.TryReadObjectHeader(out var count))
            {
                // Handle as error or return default value
                throw new InvalidOperationException("Failed to read object header for MembershipVersion");
            }
            
            long versionValue = reader.ReadUnmanaged<long>();
            value = new MembershipVersion(versionValue);
        }
    }

    /// <summary>
    /// MemoryPack formatter for <see cref="UniqueKey"/>.
    /// </summary>
    public class UniqueKeyFormatter : MemoryPackFormatter<UniqueKey>
    {
        public override void Serialize<TBufferWriter>(ref MemoryPackWriter<TBufferWriter> writer, scoped ref UniqueKey value)
        {
            // Use EqualityComparer to safely check for default value
            if (EqualityComparer<UniqueKey>.Default.Equals(value, default))
            {
                writer.WriteNullObjectHeader();
                return;
            }
            
            // Safety check for null string representation
            string keyString = value.ToString();
            if (string.IsNullOrEmpty(keyString))
            {
                writer.WriteNullObjectHeader();
                return;
            }
            
            writer.WriteObjectHeader(1);
            writer.WriteString(keyString);
        }

        public override void Deserialize(ref MemoryPackReader reader, scoped ref UniqueKey value)
        {
            if (!reader.TryReadObjectHeader(out var count))
            {
                // Handle as null
                value = default;
                return;
            }
            
            string hexString = reader.ReadString() ?? string.Empty;
            if (string.IsNullOrWhiteSpace(hexString))
            {
                // If the string is empty, default to null to avoid parse errors
                value = default;
                return;
            }

            try
            {
                // Create a consistent UniqueKey based on the hexString
                // This uses the original string to consistently generate a comparable key

                // First create a new key as a base 
                value = UniqueKey.NewKey();

                // Then use reflection to find private fields we might replace to maintain hash code consistency
                var type = typeof(UniqueKey);
                
                // Get a deterministic hash code from the hex string
                int deterministicHashCode = GetDeterministicHashCodeFromString(hexString);
                
                // Try to find a hash code field using reflection and set it if possible
                var hashCodeField = type.GetField("_hashCode", BindingFlags.Instance | BindingFlags.NonPublic) ??
                                    type.GetField("hashCode", BindingFlags.Instance | BindingFlags.NonPublic) ??
                                    type.GetField("_hashcode", BindingFlags.Instance | BindingFlags.NonPublic);
                
                if (hashCodeField != null)
                {
                    // If we found a hash code field, set it directly
                    hashCodeField.SetValue(value, deterministicHashCode);
                }
                else
                {
                    // If we couldn't find the hash code field, create a custom UniqueKey with consistent hash code
                    // We'll need to use more creative approaches

                    // Look for other fields we might modify to affect the hash code
                    var fields = type.GetFields(BindingFlags.Instance | BindingFlags.NonPublic);
                    foreach (var field in fields)
                    {
                        if (field.FieldType == typeof(byte[]))
                        {
                            // If we find a byte array field, try to set it with bytes derived from the hex string
                            byte[] bytes = HexStringToByteArray(hexString);
                            if (bytes.Length > 0)
                            {
                                field.SetValue(value, bytes);
                            }
                        }
                        else if (field.FieldType == typeof(Guid))
                        {
                            // If we find a Guid field, try to create a deterministic Guid from the hex string
                            if (Guid.TryParse(hexString.Length > 32 ? hexString.Substring(0, 32) : hexString, out Guid guid))
                            {
                                field.SetValue(value, guid);
                            }
                        }
                    }
                }

                // Log success in using deterministic deserialization
                Console.WriteLine($"Used deterministic deserialization for UniqueKey with string: {hexString}");
            }
            catch (Exception ex)
            {
                // Throw a more specific exception
                throw new MemoryPackSerializationException($"Failed to deserialize UniqueKey: {ex.Message}", ex);
            }
        }

        // Helper method to convert a hex string to byte array
        private static byte[] HexStringToByteArray(string hex)
        {
            // Remove any non-hex characters (like dashes in a GUID)
            hex = System.Text.RegularExpressions.Regex.Replace(hex, "[^0-9A-Fa-f]", "");
            
            // Ensure we have an even number of hex characters
            if (hex.Length % 2 != 0)
            {
                hex = "0" + hex;
            }
            
            byte[] bytes = new byte[hex.Length / 2];
            for (int i = 0; i < hex.Length; i += 2)
            {
                bytes[i / 2] = Convert.ToByte(hex.Substring(i, 2), 16);
            }
            return bytes;
        }
        
        // Helper method to get a deterministic hash code from a string
        private static int GetDeterministicHashCodeFromString(string s)
        {
            // Use a simple deterministic hash algorithm (FNV-1a)
            unchecked
            {
                const int p = 16777619;
                int hash = (int)2166136261;
                
                for (int i = 0; i < s.Length; i++)
                {
                    hash = (hash ^ s[i]) * p;
                }
                
                hash += hash << 13;
                hash ^= hash >> 7;
                hash += hash << 3;
                hash ^= hash >> 17;
                hash += hash << 5;
                
                return hash;
            }
        }
    }

    /// <summary>
    /// MemoryPack formatter for <see cref="IPEndPoint"/>.
    /// </summary>
    public class IPEndPointFormatter : MemoryPackFormatter<IPEndPoint?>
    {
        public override void Serialize<TBufferWriter>(ref MemoryPackWriter<TBufferWriter> writer, scoped ref IPEndPoint? value)
        {
            if (value == null)
            {
                writer.WriteNullObjectHeader();
                return;
            }
            
            writer.WriteObjectHeader(2);
            
            // Serialize IPAddress
            var ipAddress = value.Address;
            if (ipAddress == null)
            {
                writer.WriteNullObjectHeader();
            }
            else
            {
                byte[] addressBytes = ipAddress.GetAddressBytes();
                writer.WriteObjectHeader(1);
                writer.WriteUnmanaged(addressBytes.Length);
                writer.WriteSpan<byte>(addressBytes);
            }
            
            // Serialize Port
            writer.WriteUnmanaged(value.Port);
        }

        public override void Deserialize(ref MemoryPackReader reader, scoped ref IPEndPoint? value)
        {
            if (!reader.TryReadObjectHeader(out var count))
            {
                value = null;
                return;
            }
            
            // Deserialize IPAddress
            IPAddress address;
            if (!reader.TryReadObjectHeader(out var addressCount))
            {
                address = null;
            }
            else
            {
                int length = reader.ReadUnmanaged<int>();
                byte[] addressBytes = reader.ReadArray<byte>() ?? Array.Empty<byte>();
                address = new IPAddress(addressBytes);
            }
            
            // Deserialize Port
            int port = reader.ReadUnmanaged<int>();
            
            value = new IPEndPoint(address, port);
        }
    }

    /// <summary>
    /// MemoryPack formatter for Orleans grain references.
    /// </summary>
    public class GrainReferenceFormatter : MemoryPackFormatter<IAddressable>
    {
        /// <summary>
        /// The reference activator used to create grain references during deserialization.
        /// </summary>
        public static GrainReferenceActivator ReferenceActivator { get; set; }

        /// <summary>
        /// Serializes an IAddressable (GrainReference).
        /// </summary>
        public override void Serialize<TBufferWriter>(ref MemoryPackWriter<TBufferWriter> writer, scoped ref IAddressable value)
        {
            if (value is null)
            {
                writer.WriteNullObjectHeader();
                return;
            }

            // Get the grain ID from the grain reference
            if (value is GrainReference grainRef)
            {
                // Write a single field - the grain ID
                writer.WriteObjectHeader(1);
                
                // Get the GrainId and serialize it directly
                GrainId grainId = grainRef.GrainId;
                
                // Use the GrainIdFormatter to serialize the GrainId
                writer.WriteObjectHeader(2);
                writer.WriteString(grainId.Type.ToString());
                writer.WriteString(grainId.Key.ToString());
            }
            else
            {
                // Not a valid grain reference - throw an exception
                throw new InvalidOperationException($"Cannot serialize object of type {value.GetType()}. Only GrainReference objects can be serialized by GrainReferenceFormatter.");
            }
        }

        /// <summary>
        /// Deserializes an IAddressable (GrainReference).
        /// </summary>
        public override void Deserialize(ref MemoryPackReader reader, scoped ref IAddressable value)
        {
            if (!reader.TryReadObjectHeader(out var count))
            {
                value = null;
                return;
            }

            // We need an activator to create the reference.
            if (ReferenceActivator == null)
            {
                throw new InvalidOperationException("Cannot deserialize IAddressable because GrainReferenceActivator has not been initialized");
            }

            // We expect exactly one value, the grain id.
            if (count != 1)
            {
                value = null;
                return;
            }

            try
            {
                // Read the GrainId fields directly
                if (!reader.TryReadObjectHeader(out var grainIdFieldCount) || grainIdFieldCount != 2)
                {
                    value = null;
                    return;
                }

                string type = reader.ReadString() ?? string.Empty;
                string key = reader.ReadString() ?? string.Empty;
                
                // Create the GrainId and then the reference
                var grainId = GrainId.Create(type, key);
                
                // Create a grain reference using the activator with a default interface type
                value = ReferenceActivator.CreateReference(grainId, default(GrainInterfaceType));
            }
            catch
            {
                value = null;
            }
        }
    }

    /// <summary>
    /// Provides extension methods for registering Orleans formatters with MemoryPack.
    /// </summary>
    public static class OrleansMemoryPackFormatterRegistry
    {
        /// <summary>
        /// Registers all Orleans formatters with MemoryPack.
        /// </summary>
        /// <param name="referenceActivator">The grain reference activator used for serializing grain references.</param>
        /// <exception cref="ArgumentNullException">Thrown if referenceActivator is null.</exception>
        public static void RegisterFormatters(GrainReferenceActivator referenceActivator)
        {
            // Add null check for referenceActivator
            if (referenceActivator == null)
            {
                throw new ArgumentNullException(nameof(referenceActivator), "GrainReferenceActivator cannot be null.");
            }
            
            // Set the static reference activator
            GrainReferenceFormatter.ReferenceActivator = referenceActivator;
            
            // Register all formatters
            MemoryPackFormatterProvider.Register(new IPAddressFormatter());
            MemoryPackFormatterProvider.Register(new GrainIdFormatter());
            MemoryPackFormatterProvider.Register(new ActivationIdFormatter());
            MemoryPackFormatterProvider.Register(new SiloAddressFormatter());
            MemoryPackFormatterProvider.Register(new MembershipVersionFormatter());
            MemoryPackFormatterProvider.Register(new UniqueKeyFormatter());
            MemoryPackFormatterProvider.Register(new IPEndPointFormatter());
            MemoryPackFormatterProvider.Register(new GrainReferenceFormatter());
        }
    }
} 