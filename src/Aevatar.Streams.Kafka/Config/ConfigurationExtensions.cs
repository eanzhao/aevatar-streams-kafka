using Confluent.SchemaRegistry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Config;
using Orleans.Streams.Kafka.Core;
using Orleans.Streams.Kafka.Serialization;
using Orleans.Streams.Utils.Serialization;
using System;
using Aevatar.Streams.Kafka.Serialization;

// ReSharper disable once CheckNamespace
namespace Orleans.Hosting
{
	public static class ConfigurationExtensions
	{
		private const int DefaultCacheSize = 4096;

		public static KafkaStreamClientBuilder AddKafka(
			this IClientBuilder builder,
			string providerName
		)
			=> new KafkaStreamClientBuilder(builder, providerName);

		public static KafkaStreamSiloHostBuilder AddKafka(
			this ISiloBuilder builder,
			string providerName
		)
			=> new KafkaStreamSiloHostBuilder(builder, providerName);

		private static IClientBuilder AddClientProvider(
			IClientBuilder builder,
			string providerName,
			Action<OptionsBuilder<KafkaStreamOptions>> configureOptions = null
		)
		{
			builder
				.ConfigureServices(services =>
				{
					services
						.ConfigureNamedOptionForLogging<KafkaStreamOptions>(providerName)
						.ConfigureNamedOptionForLogging<HashRingStreamQueueMapperOptions>(providerName)
						.AddMemoryPack(providerName)
					;
				})
				.AddPersistentStreams(providerName, KafkaAdapterFactory.Create, stream => stream.Configure(configureOptions))
				.Configure<SimpleQueueCacheOptions>(ob => ob.CacheSize = DefaultCacheSize)
				;

			return builder;
		}

		public static IClientBuilder AddKafkaStreamProvider(
			this IClientBuilder builder,
			string providerName,
			Action<KafkaStreamOptions> configureOptions
		)
			=> AddClientProvider(builder, providerName, opt => opt.Configure(configureOptions));

		public static ISiloBuilder AddKafkaStreamProvider(
			this ISiloBuilder builder,
			string providerName,
			Action<KafkaStreamOptions> configureOptions
		) => AddSiloProvider(builder, providerName, opt => opt.Configure(configureOptions));

		private static ISiloBuilder AddSiloProvider(
			this ISiloBuilder builder,
			string providerName,
			Action<OptionsBuilder<KafkaStreamOptions>> configureOptions = null
		)
		{
			builder
				.ConfigureServices(services =>
				{
					services
						.ConfigureNamedOptionForLogging<KafkaStreamOptions>(providerName)
						.ConfigureNamedOptionForLogging<HashRingStreamQueueMapperOptions>(providerName)
						.AddMemoryPack(providerName)
					;
				})
				.AddPersistentStreams(providerName, KafkaAdapterFactory.Create,
					stream => stream.Configure(configureOptions))
				.Configure<SimpleQueueCacheOptions>(options => options.CacheSize = DefaultCacheSize);

			return builder;
		}

		public static IClientBuilder AddAvro(
			this IClientBuilder builder,
			string providerName,
			string registryUrl
		) => builder.ConfigureServices(services => services.AddAvro(providerName, registryUrl));

		public static ISiloBuilder AddAvro(
			this ISiloBuilder builder,
			string providerName,
			string registryUrl
		) => builder.ConfigureServices(services => services.AddAvro(providerName, registryUrl));

		public static IClientBuilder AddJson(
			this IClientBuilder builder,
			string providerName
		) => builder.ConfigureServices(services => services.AddJson(providerName));

		public static ISiloBuilder AddJson(
			this ISiloBuilder builder,
			string providerName
		) => builder.ConfigureServices(services => services.AddJson(providerName));

		public static IClientBuilder AddMemoryPack(
			this IClientBuilder builder,
			string providerName
		) => builder.ConfigureServices(services => services.AddMemoryPack(providerName));

		public static ISiloBuilder AddMemoryPack(
			this ISiloBuilder builder,
			string providerName
		) => builder.ConfigureServices(services => services.AddMemoryPack(providerName));
	}

	public static class ServiceCollectionExtensions
	{
		public static IServiceCollection AddJson(
			this IServiceCollection services,
			string providerName
		) => services.AddKeyedSingleton<IExternalStreamDeserializer>(
			providerName,
			(sp, key) => ActivatorUtilities.CreateInstance<JsonExternalStreamDeserializer>(sp)
		);

		public static IServiceCollection AddAvro(
			this IServiceCollection services,
			string providerName,
			string registryUrl
		) => services.AddKeyedSingleton<IExternalStreamDeserializer>(
			providerName,
			(sp, key) =>
			{
				var registryConfig = new SchemaRegistryConfig
				{
					Url = registryUrl
				};

				var registry = new CachedSchemaRegistryClient(registryConfig);
				return ActivatorUtilities.CreateInstance<AvroExternalStreamDeserializer>(sp, registry);
			}
		);

		public static IServiceCollection AddMemoryPack(
			this IServiceCollection services,
			string providerName
		) 
		{
			// Register OrleansMemoryPackSerializer
			services.AddOrleansMemoryPackSerializer();
			
			// Register the deserializer
			return services.AddKeyedSingleton<IExternalStreamDeserializer>(
				providerName,
				(sp, key) => ActivatorUtilities.CreateInstance<MemoryPackExternalStreamDeserializer>(sp)
			);
		}
	}
}