using GaussDB.NodaTime.Internal;
using GaussDB.TypeMapping;

// ReSharper disable once CheckNamespace
namespace GaussDB;

/// <summary>
/// Extension adding the NodaTime plugin to an GaussDB type mapper.
/// </summary>
public static class GaussDBNodaTimeExtensions
{
    // Note: defined for binary compatibility and GaussDBConnection.GlobalTypeMapper.
    /// <summary>
    /// Sets up NodaTime mappings for the PostgreSQL date/time types.
    /// </summary>
    /// <param name="mapper">The type mapper to set up (global or connection-specific)</param>
    public static IGaussDBTypeMapper UseNodaTime(this IGaussDBTypeMapper mapper)
    {
        mapper.AddTypeInfoResolverFactory(new NodaTimeTypeInfoResolverFactory());
        return mapper;
    }

    /// <summary>
    /// Sets up NodaTime mappings for the PostgreSQL date/time types.
    /// </summary>
    /// <param name="mapper">The type mapper to set up (global or connection-specific)</param>
    public static TMapper UseNodaTime<TMapper>(this TMapper mapper) where TMapper : IGaussDBTypeMapper
    {
        mapper.AddTypeInfoResolverFactory(new NodaTimeTypeInfoResolverFactory());
        return mapper;
    }
}
