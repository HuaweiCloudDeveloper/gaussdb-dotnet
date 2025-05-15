using System.Diagnostics.CodeAnalysis;

namespace HuaweiCloud.GaussDB.Internal;

[Experimental(GaussDBDiagnostics.ConvertersExperimental)]
public readonly struct ValueMetadata
{
    public required DataFormat Format { get; init; }
    public required Size BufferRequirement { get; init; }
    public required Size Size { get; init; }
    public object? WriteState { get; init; }
}
