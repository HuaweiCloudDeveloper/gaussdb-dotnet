using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using HuaweiCloud.GaussDB.Internal.Postgres;
using HuaweiCloud.GaussDBTypes;

namespace HuaweiCloud.GaussDB.Internal;

static class AdoSerializerHelpers
{
    public static PgTypeInfo GetTypeInfoForReading(Type type, PgTypeId pgTypeId, PgSerializerOptions options)
    {
        PgTypeInfo? typeInfo = null;
        Exception? inner = null;
        try
        {
            typeInfo = options.GetTypeInfoInternal(type, pgTypeId);
        }
        catch (Exception ex)
        {
            inner = ex;
        }
        return typeInfo ?? ThrowReadingNotSupported(type, options, pgTypeId, inner);

        // InvalidCastException thrown to align with ADO.NET convention.
        [DoesNotReturn]
        static PgTypeInfo ThrowReadingNotSupported(Type? type, PgSerializerOptions options, PgTypeId pgTypeId, Exception? inner = null)
        {
            throw new InvalidCastException(
                $"Reading{(type is null ? "" : $" as '{type.FullName}'")} is not supported for fields having DataTypeName '{options.DatabaseInfo.FindPostgresType(pgTypeId)?.DisplayName ?? "unknown"}'",
                inner);
        }
    }

    public static PgTypeInfo GetTypeInfoForWriting(Type? type, PgTypeId? pgTypeId, PgSerializerOptions options, GaussDBDbType? gaussdbDbType = null)
    {
        Debug.Assert(type != typeof(object), "Parameters of type object are not supported.");

        PgTypeInfo? typeInfo = null;
        Exception? inner = null;
        try
        {
            typeInfo = options.GetTypeInfoInternal(type, pgTypeId);
        }
        catch (Exception ex)
        {
            inner = ex;
        }
        return typeInfo ?? ThrowWritingNotSupported(type, options, pgTypeId, gaussdbDbType, inner);

        // InvalidCastException thrown to align with ADO.NET convention.
        [DoesNotReturn]
        static PgTypeInfo ThrowWritingNotSupported(Type? type, PgSerializerOptions options, PgTypeId? pgTypeId, GaussDBDbType? gaussdbDbType, Exception? inner = null)
        {
            var pgTypeString = pgTypeId is null
                ? "no GaussDBDbType or DataTypeName. Try setting one of these values to the expected database type."
                : gaussdbDbType is null
                    ? $"DataTypeName '{options.DatabaseInfo.FindPostgresType(pgTypeId.GetValueOrDefault())?.DisplayName ?? "unknown"}'"
                    : $"GaussDBDbType '{gaussdbDbType}'";

            throw new InvalidCastException(
                $"Writing{(type is null ? "" : $" values of '{type.FullName}'")} is not supported for parameters having {pgTypeString}.", inner);
        }
    }
}
