using System;
using System.Diagnostics.CodeAnalysis;
using HuaweiCloud.GaussDB.Internal.Postgres;

namespace HuaweiCloud.GaussDB.Internal;

/// <summary>
/// An GaussDB resolver for type info. Used by GaussDB to read and write values to PostgreSQL.
/// </summary>
[Experimental(GaussDBDiagnostics.ConvertersExperimental)]
public interface IPgTypeInfoResolver
{
    /// <summary>
    /// Resolve a type info for a given type and data type name, at least one value will be non-null.
    /// </summary>
    /// <param name="type">The clr type being requested.</param>
    /// <param name="dataTypeName">The postgres type being requested.</param>
    /// <param name="options">Used for configuration state and GaussDB type info or PostgreSQL type catalog lookups.</param>
    /// <returns>A result, or null if there was no match.</returns>
    PgTypeInfo? GetTypeInfo(Type? type, DataTypeName? dataTypeName, PgSerializerOptions options);
}
