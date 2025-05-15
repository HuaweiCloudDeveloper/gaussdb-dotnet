using System;
using NodaTime;
using HuaweiCloud.GaussDB.Internal;
using HuaweiCloud.GaussDB.Internal.Postgres;
using HuaweiCloud.GaussDBTypes;
using static GaussDB.Internal.PgConverterFactory;

namespace HuaweiCloud.GaussDB.NodaTime.Internal;

sealed partial class NodaTimeTypeInfoResolverFactory
{
    public override IPgTypeInfoResolver? CreateRangeResolver() => new RangeResolver();
    public override IPgTypeInfoResolver? CreateRangeArrayResolver() => new RangeArrayResolver();

    class RangeResolver : IPgTypeInfoResolver
    {
        protected static DataTypeName DateRangeDataTypeName => new("pg_catalog.daterange");
        protected static DataTypeName TimestampTzRangeDataTypeName => new("pg_catalog.tstzrange");
        protected static DataTypeName TimestampRangeDataTypeName => new("pg_catalog.tsrange");

        TypeInfoMappingCollection? _mappings;
        protected TypeInfoMappingCollection Mappings => _mappings ??= AddMappings(new());

        public PgTypeInfo? GetTypeInfo(Type? type, DataTypeName? dataTypeName, PgSerializerOptions options)
            => Mappings.Find(type, dataTypeName, options);

        static TypeInfoMappingCollection AddMappings(TypeInfoMappingCollection mappings)
        {
            // tstzrange
            mappings.AddStructType<Interval>(TimestampTzRangeDataTypeName,
                static (options, mapping, _) =>
                    mapping.CreateInfo(options,
                        new IntervalConverter(
                            CreateRangeConverter(new InstantConverter(options.EnableDateTimeInfinityConversions), options))),
                isDefault: true);
            mappings.AddStructType<GaussDBRange<Instant>>(TimestampTzRangeDataTypeName,
                static (options, mapping, _) => mapping.CreateInfo(options,
                    CreateRangeConverter(new InstantConverter(options.EnableDateTimeInfinityConversions), options)));
            mappings.AddStructType<GaussDBRange<ZonedDateTime>>(TimestampTzRangeDataTypeName,
                static (options, mapping, _) => mapping.CreateInfo(options,
                    CreateRangeConverter(new ZonedDateTimeConverter(options.EnableDateTimeInfinityConversions), options)));
            mappings.AddStructType<GaussDBRange<OffsetDateTime>>(TimestampTzRangeDataTypeName,
                static (options, mapping, _) => mapping.CreateInfo(options,
                    CreateRangeConverter(new OffsetDateTimeConverter(options.EnableDateTimeInfinityConversions), options)));

            // tsrange
            mappings.AddStructType<GaussDBRange<LocalDateTime>>(TimestampRangeDataTypeName,
                static (options, mapping, _) =>
                    mapping.CreateInfo(options,
                        CreateRangeConverter(new LocalDateTimeConverter(options.EnableDateTimeInfinityConversions), options)),
                isDefault: true);

            // daterange
            mappings.AddType<DateInterval>(DateRangeDataTypeName,
                static (options, mapping, _) =>
                    mapping.CreateInfo(options, new DateIntervalConverter(
                        CreateRangeConverter(new LocalDateConverter(options.EnableDateTimeInfinityConversions), options),
                        options.EnableDateTimeInfinityConversions)), isDefault: true);
            mappings.AddStructType<GaussDBRange<LocalDate>>(DateRangeDataTypeName,
                static (options, mapping, _) => mapping.CreateInfo(options,
                    CreateRangeConverter(new LocalDateConverter(options.EnableDateTimeInfinityConversions), options)));

            return mappings;
        }
    }

    sealed class RangeArrayResolver : RangeResolver, IPgTypeInfoResolver
    {
        TypeInfoMappingCollection? _mappings;
        new TypeInfoMappingCollection Mappings => _mappings ??= AddMappings(new(base.Mappings));

        public new PgTypeInfo? GetTypeInfo(Type? type, DataTypeName? dataTypeName, PgSerializerOptions options)
            => Mappings.Find(type, dataTypeName, options);

        static TypeInfoMappingCollection AddMappings(TypeInfoMappingCollection mappings)
        {
            // tstzrange
            mappings.AddStructArrayType<Interval>(TimestampTzRangeDataTypeName);
            mappings.AddStructArrayType<GaussDBRange<Instant>>(TimestampTzRangeDataTypeName);
            mappings.AddStructArrayType<GaussDBRange<ZonedDateTime>>(TimestampTzRangeDataTypeName);
            mappings.AddStructArrayType<GaussDBRange<OffsetDateTime>>(TimestampTzRangeDataTypeName);

            // tsrange
            mappings.AddStructArrayType<GaussDBRange<LocalDateTime>>(TimestampRangeDataTypeName);

            // daterange
            mappings.AddArrayType<DateInterval>(DateRangeDataTypeName);
            mappings.AddStructArrayType<GaussDBRange<LocalDate>>(DateRangeDataTypeName);

            return mappings;
        }
    }
}
