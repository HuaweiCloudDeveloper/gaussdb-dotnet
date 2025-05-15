using System;
using System.Data;
using HuaweiCloud.GaussDB;
using HuaweiCloud.GaussDB.Internal.Postgres;
using static HuaweiCloud.GaussDB.Util.Statics;

#pragma warning disable CA1720

// ReSharper disable once CheckNamespace
namespace HuaweiCloud.GaussDBTypes;

/// <summary>
/// Represents a PostgreSQL data type that can be written or read to the database.
/// Used in places such as <see cref="GaussDBParameter.GaussDBDbType"/> to unambiguously specify
/// how to encode or decode values.
/// </summary>
/// <remarks>
/// See https://www.postgresql.org/docs/current/static/datatype.html.
/// </remarks>
// Source for PG OIDs: <see href="https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat" />
public enum GaussDBDbType
{
    // Note that it's important to never change the numeric values of this enum, since user applications
    // compile them in.

    #region Numeric Types

    /// <summary>
    /// Corresponds to the PostgreSQL 8-byte "bigint" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-numeric.html</remarks>
    Bigint = 1,

    /// <summary>
    /// Corresponds to the PostgreSQL 8-byte floating-point "double" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-numeric.html</remarks>
    Double = 8,

    /// <summary>
    /// Corresponds to the PostgreSQL 4-byte "integer" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-numeric.html</remarks>
    Integer = 9,

    /// <summary>
    /// Corresponds to the PostgreSQL arbitrary-precision "numeric" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-numeric.html</remarks>
    Numeric = 13,

    /// <summary>
    /// Corresponds to the PostgreSQL floating-point "real" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-numeric.html</remarks>
    Real = 17,

    /// <summary>
    /// Corresponds to the PostgreSQL 2-byte "smallint" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-numeric.html</remarks>
    Smallint = 18,

    /// <summary>
    /// Corresponds to the PostgreSQL "money" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-money.html</remarks>
    Money = 12,

    #endregion

    #region Boolean Type

    /// <summary>
    /// Corresponds to the PostgreSQL "boolean" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-boolean.html</remarks>
    Boolean = 2,

    #endregion

    #region Geometric types

    /// <summary>
    /// Corresponds to the PostgreSQL geometric "box" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-geometric.html</remarks>
    Box = 3,

    /// <summary>
    /// Corresponds to the PostgreSQL geometric "circle" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-geometric.html</remarks>
    Circle = 5,

    /// <summary>
    /// Corresponds to the PostgreSQL geometric "line" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-geometric.html</remarks>
    Line = 10,

    /// <summary>
    /// Corresponds to the PostgreSQL geometric "lseg" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-geometric.html</remarks>
    LSeg = 11,

    /// <summary>
    /// Corresponds to the PostgreSQL geometric "path" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-geometric.html</remarks>
    Path = 14,

    /// <summary>
    /// Corresponds to the PostgreSQL geometric "point" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-geometric.html</remarks>
    Point = 15,

    /// <summary>
    /// Corresponds to the PostgreSQL geometric "polygon" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-geometric.html</remarks>
    Polygon = 16,

    #endregion

    #region Character Types

    /// <summary>
    /// Corresponds to the PostgreSQL "char(n)" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-character.html</remarks>
    Char = 6,

    /// <summary>
    /// Corresponds to the PostgreSQL "text" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-character.html</remarks>
    Text = 19,

    /// <summary>
    /// Corresponds to the PostgreSQL "varchar" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-character.html</remarks>
    Varchar = 22,

    /// <summary>
    /// Corresponds to the PostgreSQL internal "name" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-character.html</remarks>
    Name = 32,

    /// <summary>
    /// Corresponds to the PostgreSQL "citext" type for the citext module.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/citext.html</remarks>
    Citext = 51,   // Extension type

    /// <summary>
    /// Corresponds to the PostgreSQL "char" type.
    /// </summary>
    /// <remarks>
    /// This is an internal field and should normally not be used for regular applications.
    ///
    /// See https://www.postgresql.org/docs/current/static/datatype-text.html
    /// </remarks>
    InternalChar = 38,

    #endregion

    #region Binary Data Types

    /// <summary>
    /// Corresponds to the PostgreSQL "bytea" type, holding a raw byte string.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-binary.html</remarks>
    Bytea = 4,

    #endregion

    #region Date/Time Types

    /// <summary>
    /// Corresponds to the PostgreSQL "date" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-datetime.html</remarks>
    Date = 7,

    /// <summary>
    /// Corresponds to the PostgreSQL "time" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-datetime.html</remarks>
    Time = 20,

    /// <summary>
    /// Corresponds to the PostgreSQL "timestamp" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-datetime.html</remarks>
    Timestamp = 21,

    /// <summary>
    /// Corresponds to the PostgreSQL "timestamp with time zone" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-datetime.html</remarks>
    TimestampTz = 26,

    /// <summary>
    /// Corresponds to the PostgreSQL "interval" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-datetime.html</remarks>
    Interval = 30,

    /// <summary>
    /// Corresponds to the PostgreSQL "time with time zone" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-datetime.html</remarks>
    TimeTz = 31,

    /// <summary>
    /// Corresponds to the obsolete PostgreSQL "abstime" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-datetime.html</remarks>
    [Obsolete("The PostgreSQL abstime time is obsolete.")]
    Abstime = 33,

    #endregion

    #region Network Address Types

    /// <summary>
    /// Corresponds to the PostgreSQL "inet" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-net-types.html</remarks>
    Inet = 24,

    /// <summary>
    /// Corresponds to the PostgreSQL "cidr" type, a field storing an IPv4 or IPv6 network.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-net-types.html</remarks>
    Cidr = 44,

    /// <summary>
    /// Corresponds to the PostgreSQL "macaddr" type, a field storing a 6-byte physical address.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-net-types.html</remarks>
    MacAddr = 34,

    /// <summary>
    /// Corresponds to the PostgreSQL "macaddr8" type, a field storing a 6-byte or 8-byte physical address.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-net-types.html</remarks>
    MacAddr8 = 54,

    #endregion

    #region Bit String Types

    /// <summary>
    /// Corresponds to the PostgreSQL "bit" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-bit.html</remarks>
    Bit = 25,

    /// <summary>
    /// Corresponds to the PostgreSQL "varbit" type, a field storing a variable-length string of bits.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-boolean.html</remarks>
    Varbit = 39,

    #endregion

    #region Text Search Types

    /// <summary>
    /// Corresponds to the PostgreSQL "tsvector" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-textsearch.html</remarks>
    TsVector = 45,

    /// <summary>
    /// Corresponds to the PostgreSQL "tsquery" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-textsearch.html</remarks>
    TsQuery = 46,

    /// <summary>
    /// Corresponds to the PostgreSQL "regconfig" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-textsearch.html</remarks>
    Regconfig = 56,

    #endregion

    #region UUID Type

    /// <summary>
    /// Corresponds to the PostgreSQL "uuid" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-uuid.html</remarks>
    Uuid = 27,

    #endregion

    #region XML Type

    /// <summary>
    /// Corresponds to the PostgreSQL "xml" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-xml.html</remarks>
    Xml = 28,

    #endregion

    #region JSON Types

    /// <summary>
    /// Corresponds to the PostgreSQL "json" type, a field storing JSON in text format.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-json.html</remarks>
    /// <seealso cref="Jsonb"/>
    Json = 35,

    /// <summary>
    /// Corresponds to the PostgreSQL "jsonb" type, a field storing JSON in an optimized binary.
    /// format.
    /// </summary>
    /// <remarks>
    /// Supported since PostgreSQL 9.4.
    /// See https://www.postgresql.org/docs/current/static/datatype-json.html
    /// </remarks>
    Jsonb = 36,

    /// <summary>
    /// Corresponds to the PostgreSQL "jsonpath" type, a field storing JSON path in text format.
    /// format.
    /// </summary>
    /// <remarks>
    /// Supported since PostgreSQL 12.
    /// See https://www.postgresql.org/docs/current/datatype-json.html#DATATYPE-JSONPATH
    /// </remarks>
    JsonPath = 57,

    #endregion

    #region HSTORE Type

    /// <summary>
    /// Corresponds to the PostgreSQL "hstore" type, a dictionary of string key-value pairs.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/hstore.html</remarks>
    Hstore = 37, // Extension type

    #endregion

    #region Internal Types

    /// <summary>
    /// Corresponds to the PostgreSQL "refcursor" type.
    /// </summary>
    Refcursor = 23,

    /// <summary>
    /// Corresponds to the PostgreSQL internal "oidvector" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-oid.html</remarks>
    Oidvector = 29,

    /// <summary>
    /// Corresponds to the PostgreSQL internal "int2vector" type.
    /// </summary>
    Int2Vector = 52,

    /// <summary>
    /// Corresponds to the PostgreSQL "oid" type.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-oid.html</remarks>
    Oid = 41,

    /// <summary>
    /// Corresponds to the PostgreSQL "xid" type, an internal transaction identifier.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-oid.html</remarks>
    Xid = 42,

    /// <summary>
    /// Corresponds to the PostgreSQL "xid8" type, an internal transaction identifier.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-oid.html</remarks>
    Xid8 = 64,

    /// <summary>
    /// Corresponds to the PostgreSQL "cid" type, an internal command identifier.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/datatype-oid.html</remarks>
    Cid = 43,

    /// <summary>
    /// Corresponds to the PostgreSQL "regtype" type, a numeric (OID) ID of a type in the pg_type table.
    /// </summary>
    Regtype = 49,

    /// <summary>
    /// Corresponds to the PostgreSQL "tid" type, a tuple id identifying the physical location of a row within its table.
    /// </summary>
    Tid = 53,

    /// <summary>
    /// Corresponds to the PostgreSQL "pg_lsn" type, which can be used to store LSN (Log Sequence Number) data which
    /// is a pointer to a location in the WAL.
    /// </summary>
    /// <remarks>
    /// See: https://www.postgresql.org/docs/current/datatype-pg-lsn.html and
    /// https://git.postgresql.org/gitweb/?p=postgresql.git;a=commit;h=7d03a83f4d0736ba869fa6f93973f7623a27038a
    /// </remarks>
    PgLsn = 59,

    #endregion

    #region Special

    /// <summary>
    /// A special value that can be used to send parameter values to the database without
    /// specifying their type, allowing the database to cast them to another value based on context.
    /// The value will be converted to a string and send as text.
    /// </summary>
    /// <remarks>
    /// This value shouldn't ordinarily be used, and makes sense only when sending a data type
    /// unsupported by GaussDB.
    /// </remarks>
    Unknown = 40,

    #endregion

    #region PostGIS

    /// <summary>
    /// The geometry type for PostgreSQL spatial extension PostGIS.
    /// </summary>
    Geometry = 50,  // Extension type

    /// <summary>
    /// The geography (geodetic) type for PostgreSQL spatial extension PostGIS.
    /// </summary>
    Geography = 55, // Extension type

    #endregion

    #region Label tree types

    /// <summary>
    /// The PostgreSQL ltree type, each value is a label path "a.label.tree.value", forming a tree in a set.
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/ltree.html</remarks>
    LTree = 60, // Extension type

    /// <summary>
    /// The PostgreSQL lquery type for PostgreSQL extension ltree
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/ltree.html</remarks>
    LQuery = 61, // Extension type

    /// <summary>
    /// The PostgreSQL ltxtquery type for PostgreSQL extension ltree
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/ltree.html</remarks>
    LTxtQuery = 62, // Extension type

    #endregion

    #region Range types

    /// <summary>
    /// Corresponds to the PostgreSQL "int4range" type.
    /// </summary>
    IntegerRange = Range | Integer,

    /// <summary>
    /// Corresponds to the PostgreSQL "int8range" type.
    /// </summary>
    BigIntRange = Range | Bigint,

    /// <summary>
    /// Corresponds to the PostgreSQL "numrange" type.
    /// </summary>
    NumericRange = Range | Numeric,

    /// <summary>
    /// Corresponds to the PostgreSQL "tsrange" type.
    /// </summary>
    TimestampRange = Range | Timestamp,

    /// <summary>
    /// Corresponds to the PostgreSQL "tstzrange" type.
    /// </summary>
    TimestampTzRange = Range | TimestampTz,

    /// <summary>
    /// Corresponds to the PostgreSQL "daterange" type.
    /// </summary>
    DateRange = Range | Date,

    #endregion Range types

    #region Multirange types

    /// <summary>
    /// Corresponds to the PostgreSQL "int4multirange" type.
    /// </summary>
    IntegerMultirange = Multirange | Integer,

    /// <summary>
    /// Corresponds to the PostgreSQL "int8multirange" type.
    /// </summary>
    BigIntMultirange = Multirange | Bigint,

    /// <summary>
    /// Corresponds to the PostgreSQL "nummultirange" type.
    /// </summary>
    NumericMultirange = Multirange | Numeric,

    /// <summary>
    /// Corresponds to the PostgreSQL "tsmultirange" type.
    /// </summary>
    TimestampMultirange = Multirange | Timestamp,

    /// <summary>
    /// Corresponds to the PostgreSQL "tstzmultirange" type.
    /// </summary>
    TimestampTzMultirange = Multirange | TimestampTz,

    /// <summary>
    /// Corresponds to the PostgreSQL "datemultirange" type.
    /// </summary>
    DateMultirange = Multirange | Date,

    #endregion Multirange types

    #region Composables

    /// <summary>
    /// Corresponds to the PostgreSQL "array" type, a variable-length multidimensional array of
    /// another type. This value must be combined with another value from <see cref="GaussDBDbType"/>
    /// via a bit OR (e.g. GaussDBDbType.Array | GaussDBDbType.Integer)
    /// </summary>
    /// <remarks>See https://www.postgresql.org/docs/current/static/arrays.html</remarks>
    Array = int.MinValue,

    /// <summary>
    /// Corresponds to the PostgreSQL "range" type, continuous range of values of specific type.
    /// This value must be combined with another value from <see cref="GaussDBDbType"/>
    /// via a bit OR (e.g. GaussDBDbType.Range | GaussDBDbType.Integer)
    /// </summary>
    /// <remarks>
    /// Supported since PostgreSQL 9.2.
    /// See https://www.postgresql.org/docs/current/static/rangetypes.html
    /// </remarks>
    Range = 0x40000000,

    /// <summary>
    /// Corresponds to the PostgreSQL "multirange" type, continuous range of values of specific type.
    /// This value must be combined with another value from <see cref="GaussDBDbType"/>
    /// via a bit OR (e.g. GaussDBDbType.Multirange | GaussDBDbType.Integer)
    /// </summary>
    /// <remarks>
    /// Supported since PostgreSQL 14.
    /// See https://www.postgresql.org/docs/current/static/rangetypes.html
    /// </remarks>
    Multirange = 0x20000000,

    #endregion
}

static class GaussDBDbTypeExtensions
{
    internal static GaussDBDbType? ToGaussDBDbType(this DbType dbType)
        => dbType switch
        {
            DbType.AnsiString => GaussDBDbType.Text,
            DbType.Binary => GaussDBDbType.Bytea,
            DbType.Byte => GaussDBDbType.Smallint,
            DbType.Boolean => GaussDBDbType.Boolean,
            DbType.Currency => GaussDBDbType.Money,
            DbType.Date => GaussDBDbType.Date,
            DbType.DateTime => LegacyTimestampBehavior ? GaussDBDbType.Timestamp : GaussDBDbType.TimestampTz,
            DbType.Decimal => GaussDBDbType.Numeric,
            DbType.VarNumeric => GaussDBDbType.Numeric,
            DbType.Double => GaussDBDbType.Double,
            DbType.Guid => GaussDBDbType.Uuid,
            DbType.Int16 => GaussDBDbType.Smallint,
            DbType.Int32 => GaussDBDbType.Integer,
            DbType.Int64 => GaussDBDbType.Bigint,
            DbType.Single => GaussDBDbType.Real,
            DbType.String => GaussDBDbType.Text,
            DbType.Time => GaussDBDbType.Time,
            DbType.AnsiStringFixedLength => GaussDBDbType.Text,
            DbType.StringFixedLength => GaussDBDbType.Text,
            DbType.Xml => GaussDBDbType.Xml,
            DbType.DateTime2 => GaussDBDbType.Timestamp,
            DbType.DateTimeOffset => GaussDBDbType.TimestampTz,

            DbType.Object => null,
            DbType.SByte => null,
            DbType.UInt16 => null,
            DbType.UInt32 => null,
            DbType.UInt64 => null,

            _ => throw new ArgumentOutOfRangeException(nameof(dbType), dbType, null)
        };

    public static DbType ToDbType(this GaussDBDbType gaussdbDbType)
        => gaussdbDbType switch
        {
            // Numeric types
            GaussDBDbType.Smallint => DbType.Int16,
            GaussDBDbType.Integer => DbType.Int32,
            GaussDBDbType.Bigint => DbType.Int64,
            GaussDBDbType.Real => DbType.Single,
            GaussDBDbType.Double => DbType.Double,
            GaussDBDbType.Numeric => DbType.Decimal,
            GaussDBDbType.Money => DbType.Currency,

            // Text types
            GaussDBDbType.Text => DbType.String,
            GaussDBDbType.Xml => DbType.Xml,
            GaussDBDbType.Varchar => DbType.String,
            GaussDBDbType.Char => DbType.String,
            GaussDBDbType.Name => DbType.String,
            GaussDBDbType.Citext => DbType.String,
            GaussDBDbType.Refcursor => DbType.Object,
            GaussDBDbType.Jsonb => DbType.Object,
            GaussDBDbType.Json => DbType.Object,
            GaussDBDbType.JsonPath => DbType.Object,

            // Date/time types
            GaussDBDbType.Timestamp => LegacyTimestampBehavior ? DbType.DateTime : DbType.DateTime2,
            GaussDBDbType.TimestampTz => LegacyTimestampBehavior ? DbType.DateTimeOffset : DbType.DateTime,
            GaussDBDbType.Date => DbType.Date,
            GaussDBDbType.Time => DbType.Time,

            // Misc data types
            GaussDBDbType.Bytea => DbType.Binary,
            GaussDBDbType.Boolean => DbType.Boolean,
            GaussDBDbType.Uuid => DbType.Guid,

            GaussDBDbType.Unknown => DbType.Object,

            _ => DbType.Object
        };

    /// Can return null when a custom range type is used.
    internal static string? ToUnqualifiedDataTypeName(this GaussDBDbType gaussdbDbType)
        => gaussdbDbType switch
        {
            // Numeric types
            GaussDBDbType.Smallint => "int2",
            GaussDBDbType.Integer  => "int4",
            GaussDBDbType.Bigint   => "int8",
            GaussDBDbType.Real     => "float4",
            GaussDBDbType.Double   => "float8",
            GaussDBDbType.Numeric  => "numeric",
            GaussDBDbType.Money    => "money",

            // Text types
            GaussDBDbType.Text      => "text",
            GaussDBDbType.Xml       => "xml",
            GaussDBDbType.Varchar   => "varchar",
            GaussDBDbType.Char      => "bpchar",
            GaussDBDbType.Name      => "name",
            GaussDBDbType.Refcursor => "refcursor",
            GaussDBDbType.Jsonb     => "jsonb",
            GaussDBDbType.Json      => "json",
            GaussDBDbType.JsonPath  => "jsonpath",

            // Date/time types
            GaussDBDbType.Timestamp   => "timestamp",
            GaussDBDbType.TimestampTz => "timestamptz",
            GaussDBDbType.Date        => "date",
            GaussDBDbType.Time        => "time",
            GaussDBDbType.TimeTz      => "timetz",
            GaussDBDbType.Interval    => "interval",

            // Network types
            GaussDBDbType.Cidr     => "cidr",
            GaussDBDbType.Inet     => "inet",
            GaussDBDbType.MacAddr  => "macaddr",
            GaussDBDbType.MacAddr8 => "macaddr8",

            // Full-text search types
            GaussDBDbType.TsQuery   => "tsquery",
            GaussDBDbType.TsVector  => "tsvector",

            // Geometry types
            GaussDBDbType.Box     => "box",
            GaussDBDbType.Circle  => "circle",
            GaussDBDbType.Line    => "line",
            GaussDBDbType.LSeg    => "lseg",
            GaussDBDbType.Path    => "path",
            GaussDBDbType.Point   => "point",
            GaussDBDbType.Polygon => "polygon",


            // UInt types
            GaussDBDbType.Oid       => "oid",
            GaussDBDbType.Xid       => "xid",
            GaussDBDbType.Xid8      => "xid8",
            GaussDBDbType.Cid       => "cid",
            GaussDBDbType.Regtype   => "regtype",
            GaussDBDbType.Regconfig => "regconfig",

            // Misc types
            GaussDBDbType.Boolean => "bool",
            GaussDBDbType.Bytea   => "bytea",
            GaussDBDbType.Uuid    => "uuid",
            GaussDBDbType.Varbit  => "varbit",
            GaussDBDbType.Bit     => "bit",

            // Built-in range types
            GaussDBDbType.IntegerRange     => "int4range",
            GaussDBDbType.BigIntRange      => "int8range",
            GaussDBDbType.NumericRange     => "numrange",
            GaussDBDbType.TimestampRange   => "tsrange",
            GaussDBDbType.TimestampTzRange => "tstzrange",
            GaussDBDbType.DateRange        => "daterange",

            // Built-in multirange types
            GaussDBDbType.IntegerMultirange     => "int4multirange",
            GaussDBDbType.BigIntMultirange      => "int8multirange",
            GaussDBDbType.NumericMultirange     => "nummultirange",
            GaussDBDbType.TimestampMultirange   => "tsmultirange",
            GaussDBDbType.TimestampTzMultirange => "tstzmultirange",
            GaussDBDbType.DateMultirange        => "datemultirange",

            // Internal types
            GaussDBDbType.Int2Vector   => "int2vector",
            GaussDBDbType.Oidvector    => "oidvector",
            GaussDBDbType.PgLsn        => "pg_lsn",
            GaussDBDbType.Tid          => "tid",
            GaussDBDbType.InternalChar => "char",

            // Plugin types
            GaussDBDbType.Citext    => "citext",
            GaussDBDbType.LQuery    => "lquery",
            GaussDBDbType.LTree     => "ltree",
            GaussDBDbType.LTxtQuery => "ltxtquery",
            GaussDBDbType.Hstore    => "hstore",
            GaussDBDbType.Geometry  => "geometry",
            GaussDBDbType.Geography => "geography",

            GaussDBDbType.Unknown => "unknown",

            // Unknown cannot be composed
            _ when gaussdbDbType.HasFlag(GaussDBDbType.Array) && (gaussdbDbType & ~GaussDBDbType.Array) == GaussDBDbType.Unknown
                => "unknown",
            _ when gaussdbDbType.HasFlag(GaussDBDbType.Range) && (gaussdbDbType & ~GaussDBDbType.Range) == GaussDBDbType.Unknown
                => "unknown",
            _ when gaussdbDbType.HasFlag(GaussDBDbType.Multirange) && (gaussdbDbType & ~GaussDBDbType.Multirange) == GaussDBDbType.Unknown
                => "unknown",

            _ => gaussdbDbType.HasFlag(GaussDBDbType.Array)
                ? ToUnqualifiedDataTypeName(gaussdbDbType & ~GaussDBDbType.Array) is { } name ? "_" + name : null
                : null // e.g. ranges
        };

    internal static string ToUnqualifiedDataTypeNameOrThrow(this GaussDBDbType gaussdbDbType)
        => gaussdbDbType.ToUnqualifiedDataTypeName() ?? throw new ArgumentOutOfRangeException(nameof(gaussdbDbType), gaussdbDbType, "Cannot convert GaussDBDbType to DataTypeName");

    /// Can return null when a plugin type or custom range type is used.
    internal static DataTypeName? ToDataTypeName(this GaussDBDbType gaussdbDbType)
        => gaussdbDbType switch
        {
            // Numeric types
            GaussDBDbType.Smallint => DataTypeNames.Int2,
            GaussDBDbType.Integer => DataTypeNames.Int4,
            GaussDBDbType.Bigint => DataTypeNames.Int8,
            GaussDBDbType.Real => DataTypeNames.Float4,
            GaussDBDbType.Double => DataTypeNames.Float8,
            GaussDBDbType.Numeric => DataTypeNames.Numeric,
            GaussDBDbType.Money => DataTypeNames.Money,

            // Text types
            GaussDBDbType.Text => DataTypeNames.Text,
            GaussDBDbType.Xml => DataTypeNames.Xml,
            GaussDBDbType.Varchar => DataTypeNames.Varchar,
            GaussDBDbType.Char => DataTypeNames.Bpchar,
            GaussDBDbType.Name => DataTypeNames.Name,
            GaussDBDbType.Refcursor => DataTypeNames.RefCursor,
            GaussDBDbType.Jsonb => DataTypeNames.Jsonb,
            GaussDBDbType.Json => DataTypeNames.Json,
            GaussDBDbType.JsonPath => DataTypeNames.Jsonpath,

            // Date/time types
            GaussDBDbType.Timestamp => DataTypeNames.Timestamp,
            GaussDBDbType.TimestampTz => DataTypeNames.TimestampTz,
            GaussDBDbType.Date => DataTypeNames.Date,
            GaussDBDbType.Time => DataTypeNames.Time,
            GaussDBDbType.TimeTz => DataTypeNames.TimeTz,
            GaussDBDbType.Interval => DataTypeNames.Interval,

            // Network types
            GaussDBDbType.Cidr => DataTypeNames.Cidr,
            GaussDBDbType.Inet => DataTypeNames.Inet,
            GaussDBDbType.MacAddr => DataTypeNames.MacAddr,
            GaussDBDbType.MacAddr8 => DataTypeNames.MacAddr8,

            // Full-text search types
            GaussDBDbType.TsQuery => DataTypeNames.TsQuery,
            GaussDBDbType.TsVector => DataTypeNames.TsVector,

            // Geometry types
            GaussDBDbType.Box => DataTypeNames.Box,
            GaussDBDbType.Circle => DataTypeNames.Circle,
            GaussDBDbType.Line => DataTypeNames.Line,
            GaussDBDbType.LSeg => DataTypeNames.LSeg,
            GaussDBDbType.Path => DataTypeNames.Path,
            GaussDBDbType.Point => DataTypeNames.Point,
            GaussDBDbType.Polygon => DataTypeNames.Polygon,

            // UInt types
            GaussDBDbType.Oid => DataTypeNames.Oid,
            GaussDBDbType.Xid => DataTypeNames.Xid,
            GaussDBDbType.Xid8 => DataTypeNames.Xid8,
            GaussDBDbType.Cid => DataTypeNames.Cid,
            GaussDBDbType.Regtype => DataTypeNames.RegType,
            GaussDBDbType.Regconfig => DataTypeNames.RegConfig,

            // Misc types
            GaussDBDbType.Boolean => DataTypeNames.Bool,
            GaussDBDbType.Bytea => DataTypeNames.Bytea,
            GaussDBDbType.Uuid => DataTypeNames.Uuid,
            GaussDBDbType.Varbit => DataTypeNames.Varbit,
            GaussDBDbType.Bit => DataTypeNames.Bit,

            // Built-in range types
            GaussDBDbType.IntegerRange => DataTypeNames.Int4Range,
            GaussDBDbType.BigIntRange => DataTypeNames.Int8Range,
            GaussDBDbType.NumericRange => DataTypeNames.NumRange,
            GaussDBDbType.TimestampRange => DataTypeNames.TsRange,
            GaussDBDbType.TimestampTzRange => DataTypeNames.TsTzRange,
            GaussDBDbType.DateRange => DataTypeNames.DateRange,

            // Internal types
            GaussDBDbType.Int2Vector => DataTypeNames.Int2Vector,
            GaussDBDbType.Oidvector => DataTypeNames.OidVector,
            GaussDBDbType.PgLsn => DataTypeNames.PgLsn,
            GaussDBDbType.Tid => DataTypeNames.Tid,
            GaussDBDbType.InternalChar => DataTypeNames.Char,

            // Special types
            GaussDBDbType.Unknown => DataTypeNames.Unknown,

            // Unknown cannot be composed
            _ when gaussdbDbType.HasFlag(GaussDBDbType.Array) && (gaussdbDbType & ~GaussDBDbType.Array) == GaussDBDbType.Unknown
                => DataTypeNames.Unknown,
            _ when gaussdbDbType.HasFlag(GaussDBDbType.Range) && (gaussdbDbType & ~GaussDBDbType.Range) == GaussDBDbType.Unknown
                => DataTypeNames.Unknown,
            _ when gaussdbDbType.HasFlag(GaussDBDbType.Multirange) && (gaussdbDbType & ~GaussDBDbType.Multirange) == GaussDBDbType.Unknown
                 => DataTypeNames.Unknown,

            // If both multirange and array are set we first remove array, so array is added to the outermost datatypename.
            _ when gaussdbDbType.HasFlag(GaussDBDbType.Array)
                => ToDataTypeName(gaussdbDbType & ~GaussDBDbType.Array)?.ToArrayName(),
            _ when gaussdbDbType.HasFlag(GaussDBDbType.Multirange)
                => ToDataTypeName((gaussdbDbType | GaussDBDbType.Range) & ~GaussDBDbType.Multirange)?.ToDefaultMultirangeName(),

            // Plugin types don't have a stable fully qualified name.
            _ => null
        };

    internal static GaussDBDbType? ToGaussDBDbType(this DataTypeName dataTypeName) => ToGaussDBDbType(dataTypeName.UnqualifiedName);
    /// Should not be used with display names, first normalize it instead.
    internal static GaussDBDbType? ToGaussDBDbType(string dataTypeName)
    {
        var unqualifiedName = dataTypeName;
        if (dataTypeName.IndexOf(".", StringComparison.Ordinal) is not -1 and var index)
            unqualifiedName = dataTypeName.Substring(0, index);

        return unqualifiedName switch
            {
                // Numeric types
                "int2" => GaussDBDbType.Smallint,
                "int4" => GaussDBDbType.Integer,
                "int8" => GaussDBDbType.Bigint,
                "float4" => GaussDBDbType.Real,
                "float8" => GaussDBDbType.Double,
                "numeric" => GaussDBDbType.Numeric,
                "money" => GaussDBDbType.Money,

                // Text types
                "text" => GaussDBDbType.Text,
                "xml" => GaussDBDbType.Xml,
                "varchar" => GaussDBDbType.Varchar,
                "bpchar" => GaussDBDbType.Char,
                "name" => GaussDBDbType.Name,
                "refcursor" => GaussDBDbType.Refcursor,
                "jsonb" => GaussDBDbType.Jsonb,
                "json" => GaussDBDbType.Json,
                "jsonpath" => GaussDBDbType.JsonPath,

                // Date/time types
                "timestamp" => GaussDBDbType.Timestamp,
                "timestamptz" => GaussDBDbType.TimestampTz,
                "date" => GaussDBDbType.Date,
                "time" => GaussDBDbType.Time,
                "timetz" => GaussDBDbType.TimeTz,
                "interval" => GaussDBDbType.Interval,

                // Network types
                "cidr" => GaussDBDbType.Cidr,
                "inet" => GaussDBDbType.Inet,
                "macaddr" => GaussDBDbType.MacAddr,
                "macaddr8" => GaussDBDbType.MacAddr8,

                // Full-text search types
                "tsquery" => GaussDBDbType.TsQuery,
                "tsvector" => GaussDBDbType.TsVector,

                // Geometry types
                "box" => GaussDBDbType.Box,
                "circle" => GaussDBDbType.Circle,
                "line" => GaussDBDbType.Line,
                "lseg" => GaussDBDbType.LSeg,
                "path" => GaussDBDbType.Path,
                "point" => GaussDBDbType.Point,
                "polygon" => GaussDBDbType.Polygon,

                // UInt types
                "oid" => GaussDBDbType.Oid,
                "xid" => GaussDBDbType.Xid,
                "xid8" => GaussDBDbType.Xid8,
                "cid" => GaussDBDbType.Cid,
                "regtype" => GaussDBDbType.Regtype,
                "regconfig" => GaussDBDbType.Regconfig,

                // Misc types
                "bool" => GaussDBDbType.Boolean,
                "bytea" => GaussDBDbType.Bytea,
                "uuid" => GaussDBDbType.Uuid,
                "varbit" => GaussDBDbType.Varbit,
                "bit" => GaussDBDbType.Bit,

                // Built-in range types
                "int4range" => GaussDBDbType.IntegerRange,
                "int8range" => GaussDBDbType.BigIntRange,
                "numrange" => GaussDBDbType.NumericRange,
                "tsrange" => GaussDBDbType.TimestampRange,
                "tstzrange" => GaussDBDbType.TimestampTzRange,
                "daterange" => GaussDBDbType.DateRange,

                // Built-in multirange types
                "int4multirange" => GaussDBDbType.IntegerMultirange,
                "int8multirange" => GaussDBDbType.BigIntMultirange,
                "nummultirange" => GaussDBDbType.NumericMultirange,
                "tsmultirange" => GaussDBDbType.TimestampMultirange,
                "tstzmultirange" => GaussDBDbType.TimestampTzMultirange,
                "datemultirange" => GaussDBDbType.DateMultirange,

                // Internal types
                "int2vector" => GaussDBDbType.Int2Vector,
                "oidvector" => GaussDBDbType.Oidvector,
                "pg_lsn" => GaussDBDbType.PgLsn,
                "tid" => GaussDBDbType.Tid,
                "char" => GaussDBDbType.InternalChar,

                // Plugin types
                "citext" => GaussDBDbType.Citext,
                "lquery" => GaussDBDbType.LQuery,
                "ltree" => GaussDBDbType.LTree,
                "ltxtquery" => GaussDBDbType.LTxtQuery,
                "hstore" => GaussDBDbType.Hstore,
                "geometry" => GaussDBDbType.Geometry,
                "geography" => GaussDBDbType.Geography,

                _ when unqualifiedName.Contains("unknown")
                    => !unqualifiedName.StartsWith("_", StringComparison.Ordinal)
                        ? GaussDBDbType.Unknown
                        : null,
                _ when unqualifiedName.StartsWith("_", StringComparison.Ordinal)
                    => ToGaussDBDbType(unqualifiedName.Substring(1)) is { } elementGaussDBDbType
                        ? elementGaussDBDbType | GaussDBDbType.Array
                        : null,
                // e.g. custom ranges, plugin types etc.
                _ => null
            };
    }
}
