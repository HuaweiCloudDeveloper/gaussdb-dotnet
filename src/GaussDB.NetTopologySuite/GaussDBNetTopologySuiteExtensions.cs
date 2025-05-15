using NetTopologySuite.Geometries;
using GaussDB.NetTopologySuite.Internal;
using GaussDB.TypeMapping;

// ReSharper disable once CheckNamespace
namespace GaussDB;

/// <summary>
/// Extension allowing adding the NetTopologySuite plugin to an GaussDB type mapper.
/// </summary>
public static class GaussDBNetTopologySuiteExtensions
{
    // Note: defined for binary compatibility and GaussDBConnection.GlobalTypeMapper.
    /// <summary>
    /// Sets up NetTopologySuite mappings for the PostGIS types.
    /// </summary>
    /// <param name="mapper">The type mapper to set up (global or connection-specific).</param>
    /// <param name="coordinateSequenceFactory">The factory which knows how to build a particular implementation of ICoordinateSequence from an array of Coordinates.</param>
    /// <param name="precisionModel">Specifies the grid of allowable points.</param>
    /// <param name="handleOrdinates">Specifies the ordinates which will be handled. Not specified ordinates will be ignored.
    /// If <see cref="F:GeoAPI.Geometries.Ordiantes.None" /> is specified, an actual value will be taken from
    /// the <see cref="P:GeoAPI.Geometries.ICoordinateSequenceFactory.Ordinates"/> property of <paramref name="coordinateSequenceFactory"/>.</param>
    /// <param name="geographyAsDefault">Specifies that the geography type is used for mapping by default.</param>
    public static IGaussDBTypeMapper UseNetTopologySuite(
        this IGaussDBTypeMapper mapper,
        CoordinateSequenceFactory? coordinateSequenceFactory = null,
        PrecisionModel? precisionModel = null,
        Ordinates handleOrdinates = Ordinates.None,
        bool geographyAsDefault = false)
    {
        mapper.AddTypeInfoResolverFactory(new NetTopologySuiteTypeInfoResolverFactory(coordinateSequenceFactory, precisionModel, handleOrdinates, geographyAsDefault));
        return mapper;
    }

    /// <summary>
    /// Sets up NetTopologySuite mappings for the PostGIS types.
    /// </summary>
    /// <param name="mapper">The type mapper to set up (global or connection-specific).</param>
    /// <param name="coordinateSequenceFactory">The factory which knows how to build a particular implementation of ICoordinateSequence from an array of Coordinates.</param>
    /// <param name="precisionModel">Specifies the grid of allowable points.</param>
    /// <param name="handleOrdinates">Specifies the ordinates which will be handled. Not specified ordinates will be ignored.
    /// If <see cref="F:GeoAPI.Geometries.Ordiantes.None" /> is specified, an actual value will be taken from
    /// the <see cref="P:GeoAPI.Geometries.ICoordinateSequenceFactory.Ordinates"/> property of <paramref name="coordinateSequenceFactory"/>.</param>
    /// <param name="geographyAsDefault">Specifies that the geography type is used for mapping by default.</param>
    public static TMapper UseNetTopologySuite<TMapper>(
        this TMapper mapper,
        CoordinateSequenceFactory? coordinateSequenceFactory = null,
        PrecisionModel? precisionModel = null,
        Ordinates handleOrdinates = Ordinates.None,
        bool geographyAsDefault = false)
        where TMapper : IGaussDBTypeMapper
    {
        mapper.AddTypeInfoResolverFactory(new NetTopologySuiteTypeInfoResolverFactory(coordinateSequenceFactory, precisionModel, handleOrdinates, geographyAsDefault));
        return mapper;
    }
}
