using System;
using HuaweiCloud.GaussDB.Internal.Converters;
using HuaweiCloud.GaussDB.Internal.Postgres;
using HuaweiCloud.GaussDBTypes;

namespace HuaweiCloud.GaussDB.Internal.ResolverFactories;

sealed class GeometricTypeInfoResolverFactory : PgTypeInfoResolverFactory
{
    public override IPgTypeInfoResolver CreateResolver() => new Resolver();
    public override IPgTypeInfoResolver CreateArrayResolver() => new ArrayResolver();

    class Resolver : IPgTypeInfoResolver
    {
        TypeInfoMappingCollection? _mappings;
        protected TypeInfoMappingCollection Mappings => _mappings ??= AddMappings(new());

        public PgTypeInfo? GetTypeInfo(Type? type, DataTypeName? dataTypeName, PgSerializerOptions options)
            => Mappings.Find(type, dataTypeName, options);

        static TypeInfoMappingCollection AddMappings(TypeInfoMappingCollection mappings)
        {
            mappings.AddStructType<GaussDBPoint>(DataTypeNames.Point,
                static (options, mapping, _) => mapping.CreateInfo(options, new PointConverter()), isDefault: true);
            mappings.AddStructType<GaussDBBox>(DataTypeNames.Box,
                static (options, mapping, _) => mapping.CreateInfo(options, new BoxConverter()), isDefault: true);
            mappings.AddStructType<GaussDBPolygon>(DataTypeNames.Polygon,
                static (options, mapping, _) => mapping.CreateInfo(options, new PolygonConverter()), isDefault: true);
            mappings.AddStructType<GaussDBLine>(DataTypeNames.Line,
                static (options, mapping, _) => mapping.CreateInfo(options, new LineConverter()), isDefault: true);
            mappings.AddStructType<GaussDBLSeg>(DataTypeNames.LSeg,
                static (options, mapping, _) => mapping.CreateInfo(options, new LineSegmentConverter()), isDefault: true);
            mappings.AddStructType<GaussDBPath>(DataTypeNames.Path,
                static (options, mapping, _) => mapping.CreateInfo(options, new PathConverter()), isDefault: true);
            mappings.AddStructType<GaussDBCircle>(DataTypeNames.Circle,
                static (options, mapping, _) => mapping.CreateInfo(options, new CircleConverter()), isDefault: true);

            return mappings;
        }
    }

    sealed class ArrayResolver : Resolver, IPgTypeInfoResolver
    {
        TypeInfoMappingCollection? _mappings;
        new TypeInfoMappingCollection Mappings => _mappings ??= AddMappings(new(base.Mappings));

        public new PgTypeInfo? GetTypeInfo(Type? type, DataTypeName? dataTypeName, PgSerializerOptions options)
            => Mappings.Find(type, dataTypeName, options);

        static TypeInfoMappingCollection AddMappings(TypeInfoMappingCollection mappings)
        {
            mappings.AddStructArrayType<GaussDBPoint>(DataTypeNames.Point);
            mappings.AddStructArrayType<GaussDBBox>(DataTypeNames.Box);
            mappings.AddStructArrayType<GaussDBPolygon>(DataTypeNames.Polygon);
            mappings.AddStructArrayType<GaussDBLine>(DataTypeNames.Line);
            mappings.AddStructArrayType<GaussDBLSeg>(DataTypeNames.LSeg);
            mappings.AddStructArrayType<GaussDBPath>(DataTypeNames.Path);
            mappings.AddStructArrayType<GaussDBCircle>(DataTypeNames.Circle);

            return mappings;
        }
    }
}
