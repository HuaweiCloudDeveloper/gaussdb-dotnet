using System;
using System.Diagnostics.CodeAnalysis;
using GaussDB.TypeMapping;
using GaussDBTypes;
using Newtonsoft.Json;
using GaussDB.Json.NET.Internal;

// ReSharper disable once CheckNamespace
namespace GaussDB;

/// <summary>
/// Extension allowing adding the Json.NET plugin to an GaussDB type mapper.
/// </summary>
public static class GaussDBJsonNetExtensions
{
    // Note: defined for binary compatibility and GaussDBConnection.GlobalTypeMapper.
    /// <summary>
    /// Sets up JSON.NET mappings for the PostgreSQL json and jsonb types.
    /// </summary>
    /// <param name="mapper">The type mapper to set up.</param>
    /// <param name="settings">Optional settings to customize JSON serialization.</param>
    /// <param name="jsonbClrTypes">
    /// A list of CLR types to map to PostgreSQL <c>jsonb</c> (no need to specify <see cref="GaussDBDbType.Jsonb" />).
    /// </param>
    /// <param name="jsonClrTypes">
    /// A list of CLR types to map to PostgreSQL <c>json</c> (no need to specify <see cref="GaussDBDbType.Json" />).
    /// </param>
    [RequiresUnreferencedCode("Json serializer may perform reflection on trimmed types.")]
    [RequiresDynamicCode("Serializing arbitrary types to json can require creating new generic types or methods, which requires creating code at runtime. This may not work when AOT compiling.")]
    public static IGaussDBTypeMapper UseJsonNet(
        this IGaussDBTypeMapper mapper,
        JsonSerializerSettings? settings = null,
        Type[]? jsonbClrTypes = null,
        Type[]? jsonClrTypes = null)
    {
        // Reverse order
        mapper.AddTypeInfoResolverFactory(new JsonNetPocoTypeInfoResolverFactory(jsonbClrTypes, jsonClrTypes, settings));
        mapper.AddTypeInfoResolverFactory(new JsonNetTypeInfoResolverFactory(settings));
        return mapper;
    }

    /// <summary>
    /// Sets up JSON.NET mappings for the PostgreSQL json and jsonb types.
    /// </summary>
    /// <param name="mapper">The type mapper to set up.</param>
    /// <param name="settings">Optional settings to customize JSON serialization.</param>
    /// <param name="jsonbClrTypes">
    /// A list of CLR types to map to PostgreSQL <c>jsonb</c> (no need to specify <see cref="GaussDBDbType.Jsonb" />).
    /// </param>
    /// <param name="jsonClrTypes">
    /// A list of CLR types to map to PostgreSQL <c>json</c> (no need to specify <see cref="GaussDBDbType.Json" />).
    /// </param>
    [RequiresUnreferencedCode("Json serializer may perform reflection on trimmed types.")]
    [RequiresDynamicCode("Serializing arbitrary types to json can require creating new generic types or methods, which requires creating code at runtime. This may not work when AOT compiling.")]
    public static TMapper UseJsonNet<TMapper>(
        this TMapper mapper,
        JsonSerializerSettings? settings = null,
        Type[]? jsonbClrTypes = null,
        Type[]? jsonClrTypes = null)
        where TMapper : IGaussDBTypeMapper
    {
        // Reverse order
        mapper.AddTypeInfoResolverFactory(new JsonNetPocoTypeInfoResolverFactory(jsonbClrTypes, jsonClrTypes, settings));
        mapper.AddTypeInfoResolverFactory(new JsonNetTypeInfoResolverFactory(settings));
        return mapper;
    }
}
