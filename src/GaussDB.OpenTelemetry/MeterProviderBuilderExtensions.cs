using System;
using OpenTelemetry.Metrics;

// ReSharper disable once CheckNamespace
namespace GaussDB;

/// <summary>
/// Extension method for setting up GaussDB OpenTelemetry metrics.
/// </summary>
public static class MeterProviderBuilderExtensions
{
    /// <summary>
    /// Subscribes to the GaussDB metrics reporter to enable OpenTelemetry metrics.
    /// </summary>
    public static MeterProviderBuilder AddGaussDBInstrumentation(
        this MeterProviderBuilder builder,
        Action<GaussDBMetricsOptions>? options = null)
        => builder.AddMeter("GaussDB");
}
