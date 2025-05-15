using System;
using System.Collections.Generic;
using HuaweiCloud.GaussDB.Internal.Converters;
using HuaweiCloud.GaussDB.Internal.Postgres;
using HuaweiCloud.GaussDB.Util;
using HuaweiCloud.GaussDBTypes;
using static HuaweiCloud.GaussDB.Internal.PgConverterFactory;

namespace HuaweiCloud.GaussDB.Internal.ResolverFactories;

sealed partial class AdoTypeInfoResolverFactory
{
    public override IPgTypeInfoResolver CreateMultirangeResolver() => new MultirangeResolver();
    public override IPgTypeInfoResolver CreateMultirangeArrayResolver() => new MultirangeArrayResolver();

    class MultirangeResolver : IPgTypeInfoResolver
    {
        TypeInfoMappingCollection? _mappings;
        protected TypeInfoMappingCollection Mappings => _mappings ??= AddMappings(new());

        public PgTypeInfo? GetTypeInfo(Type? type, DataTypeName? dataTypeName, PgSerializerOptions options)
            => options.DatabaseInfo.SupportsMultirangeTypes ? Mappings.Find(type, dataTypeName, options) : null;

        static TypeInfoMappingCollection AddMappings(TypeInfoMappingCollection mappings)
        {
            // int4multirange
            mappings.AddType<GaussDBRange<int>[]>(DataTypeNames.Int4Multirange,
                static (options, mapping, _) =>
                    mapping.CreateInfo(options,
                        CreateArrayMultirangeConverter(CreateRangeConverter(new Int4Converter<int>(), options), options)),
                isDefault: true);
            mappings.AddType<List<GaussDBRange<int>>>(DataTypeNames.Int4Multirange,
                static (options, mapping, _) =>
                    mapping.CreateInfo(options,
                        CreateListMultirangeConverter(CreateRangeConverter(new Int4Converter<int>(), options), options)));

            // int8multirange
            mappings.AddType<GaussDBRange<long>[]>(DataTypeNames.Int8Multirange,
                static (options, mapping, _) =>
                    mapping.CreateInfo(options,
                        CreateArrayMultirangeConverter(CreateRangeConverter(new Int8Converter<long>(), options), options)),
                isDefault: true);
            mappings.AddType<List<GaussDBRange<long>>>(DataTypeNames.Int8Multirange,
                static (options, mapping, _) =>
                    mapping.CreateInfo(options,
                        CreateListMultirangeConverter(CreateRangeConverter(new Int8Converter<long>(), options), options)));

            // nummultirange
            mappings.AddType<GaussDBRange<decimal>[]>(DataTypeNames.NumMultirange,
                static (options, mapping, _) =>
                    mapping.CreateInfo(options,
                        CreateArrayMultirangeConverter(CreateRangeConverter(new DecimalNumericConverter<decimal>(), options), options)),
                isDefault: true);
            mappings.AddType<List<GaussDBRange<decimal>>>(DataTypeNames.NumMultirange,
                static (options, mapping, _) =>
                    mapping.CreateInfo(options,
                        CreateListMultirangeConverter(CreateRangeConverter(new DecimalNumericConverter<decimal>(), options), options)));

            // tsmultirange
            if (Statics.LegacyTimestampBehavior)
            {
                mappings.AddType<GaussDBRange<DateTime>[]>(DataTypeNames.TsMultirange,
                    static (options, mapping, _) =>
                        mapping.CreateInfo(options, CreateArrayMultirangeConverter(
                            CreateRangeConverter(new LegacyDateTimeConverter(options.EnableDateTimeInfinityConversions, timestamp: true),
                                options), options)),
                    isDefault: true);
                mappings.AddType<List<GaussDBRange<DateTime>>>(DataTypeNames.TsMultirange,
                    static (options, mapping, _) =>
                        mapping.CreateInfo(options, CreateListMultirangeConverter(
                            CreateRangeConverter(new LegacyDateTimeConverter(options.EnableDateTimeInfinityConversions, timestamp: true),
                                options), options)));
            }
            else
            {
                mappings.AddResolverType<GaussDBRange<DateTime>[]>(DataTypeNames.TsMultirange,
                    static (options, mapping, requiresDataTypeName) => mapping.CreateInfo(options,
                        DateTimeConverterResolver.CreateMultirangeResolver<GaussDBRange<DateTime>[], GaussDBRange<DateTime>>(options,
                            options.GetCanonicalTypeId(DataTypeNames.TsTzMultirange),
                            options.GetCanonicalTypeId(DataTypeNames.TsMultirange),
                            options.EnableDateTimeInfinityConversions), requiresDataTypeName),
                    isDefault: true);
                mappings.AddResolverType<List<GaussDBRange<DateTime>>>(DataTypeNames.TsMultirange,
                    static (options, mapping, requiresDataTypeName) => mapping.CreateInfo(options,
                        DateTimeConverterResolver.CreateMultirangeResolver<List<GaussDBRange<DateTime>>, GaussDBRange<DateTime>>(options,
                            options.GetCanonicalTypeId(DataTypeNames.TsTzMultirange),
                            options.GetCanonicalTypeId(DataTypeNames.TsMultirange),
                            options.EnableDateTimeInfinityConversions), requiresDataTypeName));
            }

            mappings.AddType<GaussDBRange<long>[]>(DataTypeNames.TsMultirange,
                static (options, mapping, _) =>
                    mapping.CreateInfo(options,
                        CreateArrayMultirangeConverter(CreateRangeConverter(new Int8Converter<long>(), options), options)));
            mappings.AddType<List<GaussDBRange<long>>>(DataTypeNames.TsMultirange,
                static (options, mapping, _) =>
                    mapping.CreateInfo(options,
                        CreateListMultirangeConverter(CreateRangeConverter(new Int8Converter<long>(), options), options)));

            // tstzmultirange
            if (Statics.LegacyTimestampBehavior)
            {
                mappings.AddType<GaussDBRange<DateTime>[]>(DataTypeNames.TsTzMultirange,
                    static (options, mapping, _) =>
                        mapping.CreateInfo(options, CreateArrayMultirangeConverter(
                            CreateRangeConverter(new LegacyDateTimeConverter(options.EnableDateTimeInfinityConversions, timestamp: false),
                                options), options)),
                    isDefault: true);
                mappings.AddType<List<GaussDBRange<DateTime>>>(DataTypeNames.TsTzMultirange,
                    static (options, mapping, _) =>
                        mapping.CreateInfo(options, CreateListMultirangeConverter(
                            CreateRangeConverter(new LegacyDateTimeConverter(options.EnableDateTimeInfinityConversions, timestamp: false),
                                options), options)));
                mappings.AddType<GaussDBRange<DateTimeOffset>[]>(DataTypeNames.TsTzMultirange,
                    static (options, mapping, _) =>
                        mapping.CreateInfo(options, CreateArrayMultirangeConverter(
                            CreateRangeConverter(new LegacyDateTimeOffsetConverter(options.EnableDateTimeInfinityConversions), options),
                            options)),
                    isDefault: true);
                mappings.AddType<List<GaussDBRange<DateTimeOffset>>>(DataTypeNames.TsTzMultirange,
                    static (options, mapping, _) =>
                        mapping.CreateInfo(options, CreateListMultirangeConverter(
                            CreateRangeConverter(new LegacyDateTimeOffsetConverter(options.EnableDateTimeInfinityConversions), options),
                            options)));
            }
            else
            {
                mappings.AddResolverType<GaussDBRange<DateTime>[]>(DataTypeNames.TsTzMultirange,
                    static (options, mapping, requiresDataTypeName) => mapping.CreateInfo(options,
                        DateTimeConverterResolver.CreateMultirangeResolver<GaussDBRange<DateTime>[], GaussDBRange<DateTime>>(options,
                            options.GetCanonicalTypeId(DataTypeNames.TsTzMultirange),
                            options.GetCanonicalTypeId(DataTypeNames.TsMultirange),
                            options.EnableDateTimeInfinityConversions), requiresDataTypeName),
                    isDefault: true);
                mappings.AddResolverType<List<GaussDBRange<DateTime>>>(DataTypeNames.TsTzMultirange,
                    static (options, mapping, requiresDataTypeName) => mapping.CreateInfo(options,
                        DateTimeConverterResolver.CreateMultirangeResolver<List<GaussDBRange<DateTime>>, GaussDBRange<DateTime>>(options,
                            options.GetCanonicalTypeId(DataTypeNames.TsTzMultirange),
                            options.GetCanonicalTypeId(DataTypeNames.TsMultirange),
                            options.EnableDateTimeInfinityConversions), requiresDataTypeName));
                mappings.AddType<GaussDBRange<DateTimeOffset>[]>(DataTypeNames.TsTzMultirange,
                    static (options, mapping, _) =>
                        mapping.CreateInfo(options, CreateArrayMultirangeConverter(
                            CreateRangeConverter(new DateTimeOffsetConverter(options.EnableDateTimeInfinityConversions), options), options)),
                    isDefault: true);
                mappings.AddType<List<GaussDBRange<DateTimeOffset>>>(DataTypeNames.TsTzMultirange,
                    static (options, mapping, _) =>
                        mapping.CreateInfo(options, CreateListMultirangeConverter(
                            CreateRangeConverter(new DateTimeOffsetConverter(options.EnableDateTimeInfinityConversions), options), options)));
            }

            mappings.AddType<GaussDBRange<long>[]>(DataTypeNames.TsTzMultirange,
                static (options, mapping, _) =>
                    mapping.CreateInfo(options,
                        CreateArrayMultirangeConverter(CreateRangeConverter(new Int8Converter<long>(), options), options)));
            mappings.AddType<List<GaussDBRange<long>>>(DataTypeNames.TsTzMultirange,
                static (options, mapping, _) =>
                    mapping.CreateInfo(options,
                        CreateListMultirangeConverter(CreateRangeConverter(new Int8Converter<long>(), options), options)));

            // datemultirange
            mappings.AddType<GaussDBRange<DateOnly>[]>(DataTypeNames.DateMultirange,
                static (options, mapping, _) =>
                    mapping.CreateInfo(options, CreateArrayMultirangeConverter(
                        CreateRangeConverter(new DateOnlyDateConverter(options.EnableDateTimeInfinityConversions), options), options)),
                isDefault: true);
            mappings.AddType<GaussDBRange<DateTime>[]>(DataTypeNames.DateMultirange,
                static (options, mapping, _) =>
                    mapping.CreateInfo(options, CreateArrayMultirangeConverter(
                        CreateRangeConverter(new DateTimeDateConverter(options.EnableDateTimeInfinityConversions), options), options)));
            mappings.AddType<List<GaussDBRange<DateOnly>>>(DataTypeNames.DateMultirange,
                static (options, mapping, _) =>
                    mapping.CreateInfo(options, CreateListMultirangeConverter(
                        CreateRangeConverter(new DateOnlyDateConverter(options.EnableDateTimeInfinityConversions), options), options)));
            mappings.AddType<List<GaussDBRange<DateTime>>>(DataTypeNames.DateMultirange,
                static (options, mapping, _) =>
                    mapping.CreateInfo(options, CreateListMultirangeConverter(
                        CreateRangeConverter(new DateTimeDateConverter(options.EnableDateTimeInfinityConversions), options), options)));

            return mappings;
        }
    }

    sealed class MultirangeArrayResolver : MultirangeResolver, IPgTypeInfoResolver
    {
        TypeInfoMappingCollection? _mappings;
        new TypeInfoMappingCollection Mappings => _mappings ??= AddMappings(new(base.Mappings));

        public new PgTypeInfo? GetTypeInfo(Type? type, DataTypeName? dataTypeName, PgSerializerOptions options)
            => options.DatabaseInfo.SupportsMultirangeTypes ? Mappings.Find(type, dataTypeName, options) : null;

        static TypeInfoMappingCollection AddMappings(TypeInfoMappingCollection mappings)
        {
            // int4multirange
            mappings.AddArrayType<GaussDBRange<int>[]>(DataTypeNames.Int4Multirange);
            mappings.AddArrayType<List<GaussDBRange<int>>>(DataTypeNames.Int4Multirange);

            // int8multirange
            mappings.AddArrayType<GaussDBRange<long>[]>(DataTypeNames.Int8Multirange);
            mappings.AddArrayType<List<GaussDBRange<long>>>(DataTypeNames.Int8Multirange);

            // nummultirange
            mappings.AddArrayType<GaussDBRange<decimal>[]>(DataTypeNames.NumMultirange);
            mappings.AddArrayType<List<GaussDBRange<decimal>>>(DataTypeNames.NumMultirange);

            // tsmultirange
            if (Statics.LegacyTimestampBehavior)
            {
                mappings.AddArrayType<GaussDBRange<DateTime>[]>(DataTypeNames.TsMultirange);
                mappings.AddArrayType<List<GaussDBRange<DateTime>>>(DataTypeNames.TsMultirange);
            }
            else
            {
                mappings.AddResolverArrayType<GaussDBRange<DateTime>[]>(DataTypeNames.TsMultirange);
                mappings.AddResolverArrayType<List<GaussDBRange<DateTime>>>(DataTypeNames.TsMultirange);
            }

            mappings.AddArrayType<GaussDBRange<long>[]>(DataTypeNames.TsMultirange);
            mappings.AddArrayType<List<GaussDBRange<long>>>(DataTypeNames.TsMultirange);

            // tstzmultirange
            if (Statics.LegacyTimestampBehavior)
            {
                mappings.AddArrayType<GaussDBRange<DateTime>[]>(DataTypeNames.TsTzMultirange);
                mappings.AddArrayType<List<GaussDBRange<DateTime>>>(DataTypeNames.TsTzMultirange);
                mappings.AddArrayType<GaussDBRange<DateTimeOffset>[]>(DataTypeNames.TsTzMultirange);
                mappings.AddArrayType<List<GaussDBRange<DateTimeOffset>>>(DataTypeNames.TsTzMultirange);
            }
            else
            {
                mappings.AddResolverArrayType<GaussDBRange<DateTime>[]>(DataTypeNames.TsTzMultirange);
                mappings.AddResolverArrayType<List<GaussDBRange<DateTime>>>(DataTypeNames.TsTzMultirange);
                mappings.AddArrayType<GaussDBRange<DateTimeOffset>[]>(DataTypeNames.TsTzMultirange);
                mappings.AddArrayType<List<GaussDBRange<DateTimeOffset>>>(DataTypeNames.TsTzMultirange);
            }

            mappings.AddArrayType<GaussDBRange<long>[]>(DataTypeNames.TsTzMultirange);
            mappings.AddArrayType<List<GaussDBRange<long>>>(DataTypeNames.TsTzMultirange);

            // datemultirange
            mappings.AddArrayType<GaussDBRange<DateTime>[]>(DataTypeNames.DateMultirange);
            mappings.AddArrayType<List<GaussDBRange<DateTime>>>(DataTypeNames.DateMultirange);
            mappings.AddArrayType<GaussDBRange<DateOnly>[]>(DataTypeNames.DateMultirange);
            mappings.AddArrayType<List<GaussDBRange<DateOnly>>>(DataTypeNames.DateMultirange);

            return mappings;
        }
    }
}
