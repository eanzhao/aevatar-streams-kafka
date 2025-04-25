using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Serialization;
using System;

namespace Orleans.Hosting
{
    public static class MemoryPackSerializerExtensions
    {
        /// <summary>
        /// Registers OrleansMemoryPackSerializer as a service in the DI container
        /// </summary>
        public static IServiceCollection AddOrleansMemoryPackSerializer(this IServiceCollection services)
        {
            services.TryAddSingleton<OrleansMemoryPackSerializer>(sp =>
            {
                var options = sp.GetService<IOptions<OrleansMemoryPackSerializerOptions>>() ??
                            Options.Create(new OrleansMemoryPackSerializerOptions());
                return new OrleansMemoryPackSerializer(options);
            });
            
            return services;
        }
        
        /// <summary>
        /// Registers OrleansMemoryPackSerializer as a service in the DI container with specified options
        /// </summary>
        public static IServiceCollection AddOrleansMemoryPackSerializer(
            this IServiceCollection services,
            Action<OrleansMemoryPackSerializerOptions> configureOptions)
        {
            services.Configure(configureOptions);
            return services.AddOrleansMemoryPackSerializer();
        }
    }
} 