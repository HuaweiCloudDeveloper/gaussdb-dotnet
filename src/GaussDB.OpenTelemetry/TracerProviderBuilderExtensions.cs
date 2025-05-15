using System;
using OpenTelemetry.Trace;

// ReSharper disable once CheckNamespace
namespace HuaweiCloud.GaussDB;

/// <summary>
/// Extension method for setting up GaussDB OpenTelemetry tracing.
/// </summary>
public static class TracerProviderBuilderExtensions
{
    /// <summary>
    /// Subscribes to the GaussDB activity source to enable OpenTelemetry tracing.
    /// </summary>
    public static TracerProviderBuilder AddGaussDB(this TracerProviderBuilder builder)
        => builder.AddSource("GaussDB");
}
