using System;
using System.Diagnostics;

namespace HuaweiCloud.GaussDB;

/// <summary>
/// A builder to configure GaussDB's support for OpenTelemetry tracing.
/// </summary>
public sealed class GaussDBTracingOptionsBuilder
{
    Func<GaussDBCommand, bool>? _commandFilter;
    Func<GaussDBBatch, bool>? _batchFilter;
    Action<Activity, GaussDBCommand>? _commandEnrichmentCallback;
    Action<Activity, GaussDBBatch>? _batchEnrichmentCallback;
    Func<GaussDBCommand, string?>? _commandSpanNameProvider;
    Func<GaussDBBatch, string?>? _batchSpanNameProvider;
    bool _enableFirstResponseEvent = true;
    bool _enablePhysicalOpenTracing = true;

    internal GaussDBTracingOptionsBuilder()
    {
    }

    /// <summary>
    /// Configures a filter function that determines whether to emit tracing information for an <see cref="GaussDBCommand"/>.
    /// By default, tracing information is emitted for all commands.
    /// </summary>
    public GaussDBTracingOptionsBuilder ConfigureCommandFilter(Func<GaussDBCommand, bool>? commandFilter)
    {
        _commandFilter = commandFilter;
        return this;
    }

    /// <summary>
    /// Configures a filter function that determines whether to emit tracing information for an <see cref="GaussDBBatch"/>.
    /// By default, tracing information is emitted for all batches.
    /// </summary>
    public GaussDBTracingOptionsBuilder ConfigureBatchFilter(Func<GaussDBBatch, bool>? batchFilter)
    {
        _batchFilter = batchFilter;
        return this;
    }

    /// <summary>
    /// Configures a callback that can enrich the <see cref="Activity"/> emitted for the given <see cref="GaussDBCommand"/>.
    /// </summary>
    public GaussDBTracingOptionsBuilder ConfigureCommandEnrichmentCallback(Action<Activity, GaussDBCommand>? commandEnrichmentCallback)
    {
        _commandEnrichmentCallback = commandEnrichmentCallback;
        return this;
    }

    /// <summary>
    /// Configures a callback that can enrich the <see cref="Activity"/> emitted for the given <see cref="GaussDBBatch"/>.
    /// </summary>
    public GaussDBTracingOptionsBuilder ConfigureBatchEnrichmentCallback(Action<Activity, GaussDBBatch>? batchEnrichmentCallback)
    {
        _batchEnrichmentCallback = batchEnrichmentCallback;
        return this;
    }

    /// <summary>
    /// Configures a callback that provides the tracing span's name for an <see cref="GaussDBCommand"/>. If <c>null</c>, the default standard
    /// span name is used, which is the database name.
    /// </summary>
    public GaussDBTracingOptionsBuilder ConfigureCommandSpanNameProvider(Func<GaussDBCommand, string?>? commandSpanNameProvider)
    {
        _commandSpanNameProvider = commandSpanNameProvider;
        return this;
    }

    /// <summary>
    /// Configures a callback that provides the tracing span's name for an <see cref="GaussDBBatch"/>. If <c>null</c>, the default standard
    /// span name is used, which is the database name.
    /// </summary>
    public GaussDBTracingOptionsBuilder ConfigureBatchSpanNameProvider(Func<GaussDBBatch, string?>? batchSpanNameProvider)
    {
        _batchSpanNameProvider = batchSpanNameProvider;
        return this;
    }

    /// <summary>
    /// Gets or sets a value indicating whether to enable the "time-to-first-read" event.
    /// Default is true to preserve existing behavior.
    /// </summary>
    public GaussDBTracingOptionsBuilder EnableFirstResponseEvent(bool enable = true)
    {
        _enableFirstResponseEvent = enable;
        return this;
    }

    /// <summary>
    /// Gets or sets a value indicating whether to trace physical connection open.
    /// Default is true to preserve existing behavior.
    /// </summary>
    public GaussDBTracingOptionsBuilder EnablePhysicalOpenTracing(bool enable = true)
    {
        _enablePhysicalOpenTracing = enable;
        return this;
    }

    internal GaussDBTracingOptions Build() => new()
    {
        CommandFilter = _commandFilter,
        BatchFilter = _batchFilter,
        CommandEnrichmentCallback = _commandEnrichmentCallback,
        BatchEnrichmentCallback = _batchEnrichmentCallback,
        CommandSpanNameProvider = _commandSpanNameProvider,
        BatchSpanNameProvider = _batchSpanNameProvider,
        EnableFirstResponseEvent = _enableFirstResponseEvent,
        EnablePhysicalOpenTracing = _enablePhysicalOpenTracing
    };
}

sealed class GaussDBTracingOptions
{
    internal Func<GaussDBCommand, bool>? CommandFilter { get; init; }
    internal Func<GaussDBBatch, bool>? BatchFilter { get; init; }
    internal Action<Activity, GaussDBCommand>? CommandEnrichmentCallback { get; init; }
    internal Action<Activity, GaussDBBatch>? BatchEnrichmentCallback { get; init; }
    internal Func<GaussDBCommand, string?>? CommandSpanNameProvider { get; init; }
    internal Func<GaussDBBatch, string?>? BatchSpanNameProvider { get; init; }
    internal bool EnableFirstResponseEvent { get; init; }
    internal bool EnablePhysicalOpenTracing { get; init; }
}
