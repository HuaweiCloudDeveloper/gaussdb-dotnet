using System;
using HuaweiCloud.GaussDB.Internal.Converters;
using HuaweiCloud.GaussDB.Internal.Postgres;
using HuaweiCloud.GaussDB.Properties;
using HuaweiCloud.GaussDBTypes;

namespace HuaweiCloud.GaussDB.Internal.ResolverFactories;

sealed class FullTextSearchTypeInfoResolverFactory : PgTypeInfoResolverFactory
{
    public override IPgTypeInfoResolver CreateResolver() => new Resolver();
    public override IPgTypeInfoResolver CreateArrayResolver() => new ArrayResolver();

    public static void ThrowIfUnsupported<TBuilder>(Type? type, DataTypeName? dataTypeName, PgSerializerOptions options)
    {
        if (dataTypeName is { SchemaSpan: "pg_catalog", UnqualifiedNameSpan: "tsquery" or "_tsquery" or "tsvector" or "_tsvector" })
            throw new NotSupportedException(
                string.Format(GaussDBStrings.FullTextSearchNotEnabled, nameof(GaussDBSlimDataSourceBuilder.EnableFullTextSearch), typeof(TBuilder).Name));

        if (type is null)
            return;

        if (TypeInfoMappingCollection.IsArrayLikeType(type, out var elementType))
            type = elementType;

        if (Nullable.GetUnderlyingType(type) is { } underlyingType)
            type = underlyingType;

        if (type == typeof(GaussDBTsVector) || typeof(GaussDBTsQuery).IsAssignableFrom(type))
            throw new NotSupportedException(
                string.Format(GaussDBStrings.FullTextSearchNotEnabled, nameof(GaussDBSlimDataSourceBuilder.EnableFullTextSearch), typeof(TBuilder).Name));
    }

    class Resolver : IPgTypeInfoResolver
    {
        TypeInfoMappingCollection? _mappings;
        protected TypeInfoMappingCollection Mappings => _mappings ??= AddMappings(new());

        public PgTypeInfo? GetTypeInfo(Type? type, DataTypeName? dataTypeName, PgSerializerOptions options)
            => Mappings.Find(type, dataTypeName, options);

        static TypeInfoMappingCollection AddMappings(TypeInfoMappingCollection mappings)
        {
            // tsvector
            mappings.AddType<GaussDBTsVector>(DataTypeNames.TsVector,
                static (options, mapping, _) => mapping.CreateInfo(options, new TsVectorConverter(options.TextEncoding)), isDefault: true);

            // tsquery
            mappings.AddType<GaussDBTsQuery>(DataTypeNames.TsQuery,
                static (options, mapping, _) => mapping.CreateInfo(options, new TsQueryConverter<GaussDBTsQuery>(options.TextEncoding)), isDefault: true);
            mappings.AddType<GaussDBTsQueryEmpty>(DataTypeNames.TsQuery,
                static (options, mapping, _) => mapping.CreateInfo(options, new TsQueryConverter<GaussDBTsQueryEmpty>(options.TextEncoding)));
            mappings.AddType<GaussDBTsQueryLexeme>(DataTypeNames.TsQuery,
                static (options, mapping, _) => mapping.CreateInfo(options, new TsQueryConverter<GaussDBTsQueryLexeme>(options.TextEncoding)));
            mappings.AddType<GaussDBTsQueryNot>(DataTypeNames.TsQuery,
                static (options, mapping, _) => mapping.CreateInfo(options, new TsQueryConverter<GaussDBTsQueryNot>(options.TextEncoding)));
            mappings.AddType<GaussDBTsQueryAnd>(DataTypeNames.TsQuery,
                static (options, mapping, _) => mapping.CreateInfo(options, new TsQueryConverter<GaussDBTsQueryAnd>(options.TextEncoding)));
            mappings.AddType<GaussDBTsQueryOr>(DataTypeNames.TsQuery,
                static (options, mapping, _) => mapping.CreateInfo(options, new TsQueryConverter<GaussDBTsQueryOr>(options.TextEncoding)));
            mappings.AddType<GaussDBTsQueryFollowedBy>(DataTypeNames.TsQuery,
                static (options, mapping, _) => mapping.CreateInfo(options, new TsQueryConverter<GaussDBTsQueryFollowedBy>(options.TextEncoding)));

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
            // tsvector
            mappings.AddArrayType<GaussDBTsVector>(DataTypeNames.TsVector);

            // tsquery
            mappings.AddArrayType<GaussDBTsQuery>(DataTypeNames.TsQuery);
            mappings.AddArrayType<GaussDBTsQueryEmpty>(DataTypeNames.TsQuery);
            mappings.AddArrayType<GaussDBTsQueryLexeme>(DataTypeNames.TsQuery);
            mappings.AddArrayType<GaussDBTsQueryNot>(DataTypeNames.TsQuery);
            mappings.AddArrayType<GaussDBTsQueryAnd>(DataTypeNames.TsQuery);
            mappings.AddArrayType<GaussDBTsQueryOr>(DataTypeNames.TsQuery);
            mappings.AddArrayType<GaussDBTsQueryFollowedBy>(DataTypeNames.TsQuery);

            return mappings;
        }
    }
}
