using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using HuaweiCloud.GaussDB.BackendMessages;
using HuaweiCloud.GaussDB.Internal;
using HuaweiCloud.GaussDB.Internal.Postgres;
using HuaweiCloud.GaussDB.PostgresTypes;
using HuaweiCloud.GaussDB.Util;
using HuaweiCloud.GaussDBTypes;

namespace HuaweiCloud.GaussDB.Schema;

sealed class DbColumnSchemaGenerator
{
    readonly RowDescriptionMessage _rowDescription;
    readonly GaussDBConnection _connection;
    readonly bool _fetchAdditionalInfo;

    internal DbColumnSchemaGenerator(GaussDBConnection connection, RowDescriptionMessage rowDescription, bool fetchAdditionalInfo)
    {
        _connection = connection;
        _rowDescription = rowDescription;
        _fetchAdditionalInfo = fetchAdditionalInfo;
    }

    #region Columns queries

    //todo: 因为pg_column_is_updatable函数不存在，简化验证逻辑仅允许普通表或分区表。
    static string GenerateColumnsQuery(Version pgVersion, string columnFieldFilter) =>
        $@"SELECT
     typ.oid AS typoid, nspname, relname, attname, attrelid, attnum, attnotnull,
     {(pgVersion.IsGreaterOrEqual(10) ? "attidentity != ''" : "FALSE")} AS isidentity,
     CASE WHEN typ.typtype = 'd' THEN typ.typtypmod ELSE atttypmod END AS typmod,
     CASE WHEN atthasdef THEN (SELECT pg_get_expr(adbin, cls.oid) FROM pg_attrdef WHERE adrelid = cls.oid AND adnum = attr.attnum) ELSE NULL END AS default,
     CASE WHEN ((cls.relkind = ANY (ARRAY['r'::""char"", 'p'::""char""]))
               OR ((cls.relkind = ANY (ARRAY['v'::""char"", 'f'::""char""]))
               AND pg_column_is_updatable((cls.oid)::regclass, attr.attnum, false)))
  	           -- 移除attidentity条件，或替换为GaussDB支持的逻辑
               THEN 'true'::boolean
               ELSE 'false'::boolean
               END AS is_updatable,
     EXISTS (
       SELECT * FROM pg_index
       WHERE pg_index.indrelid = cls.oid AND
             pg_index.indisprimary AND
             attnum = ANY (indkey)
     ) AS isprimarykey,
     EXISTS (
       SELECT * FROM pg_index
       WHERE pg_index.indrelid = cls.oid AND
             pg_index.indisunique AND
             pg_index.{(pgVersion.IsGreaterOrEqual(11) ? "indnkeyatts" : "indnatts")} = 1 AND
             attnum = pg_index.indkey[0]
     ) AS isunique
FROM pg_attribute AS attr
JOIN pg_type AS typ ON attr.atttypid = typ.oid
JOIN pg_class AS cls ON cls.oid = attr.attrelid
JOIN pg_namespace AS ns ON ns.oid = cls.relnamespace
WHERE
     atttypid <> 0 AND
     relkind IN ('r', 'v', 'm') AND
     NOT attisdropped AND
     nspname NOT IN ('pg_catalog', 'information_schema') AND
     attnum > 0 AND
     ({columnFieldFilter})
ORDER BY attnum";

    /// <summary>
    /// Stripped-down version of <see cref="GenerateColumnsQuery"/>, mainly to support Amazon Redshift.
    /// </summary>
    static string GenerateOldColumnsQuery(string columnFieldFilter) =>
        $@"SELECT
     typ.oid AS typoid, nspname, relname, attname, attrelid, attnum, attnotnull,
     CASE WHEN typ.typtype = 'd' THEN typ.typtypmod ELSE atttypmod END AS typmod,
     CASE WHEN atthasdef THEN (SELECT pg_get_expr(adbin, cls.oid) FROM pg_attrdef WHERE adrelid = cls.oid AND adnum = attr.attnum) ELSE NULL END AS default,
     TRUE AS is_updatable,  /* Supported only since PG 8.2 */
     FALSE AS isprimarykey, /* Can't do ANY() on pg_index.indkey which is int2vector */
     FALSE AS isunique      /* Can't do ANY() on pg_index.indkey which is int2vector */
FROM pg_attribute AS attr
JOIN pg_type AS typ ON attr.atttypid = typ.oid
JOIN pg_class AS cls ON cls.oid = attr.attrelid
JOIN pg_namespace AS ns ON ns.oid = cls.relnamespace
WHERE
     atttypid <> 0 AND
     relkind IN ('r', 'v', 'm') AND
     NOT attisdropped AND
     nspname NOT IN ('pg_catalog', 'information_schema') AND
     attnum > 0 AND
     ({columnFieldFilter})
ORDER BY attnum";

    #endregion Column queries

    internal async Task<ReadOnlyCollection<GaussDBDbColumn>> GetColumnSchema(bool async, CancellationToken cancellationToken = default)
    {
        // This is mainly for Amazon Redshift
        var oldQueryMode = _connection.PostgreSqlVersion < new Version(8, 2);

        var numFields = _rowDescription.Count;
        var result = new List<GaussDBDbColumn?>(numFields);
        for (var i = 0; i < numFields; i++)
            result.Add(null);
        var populatedColumns = 0;

        if (_fetchAdditionalInfo)
        {
            // We have two types of fields - those which correspond to actual database columns
            // and those that don't (e.g. SELECT 8). For the former we load lots of info from
            // the backend (if fetchAdditionalInfo is true), for the latter we only have the RowDescription

            var filters = new List<string>();
            for (var index = 0; index < _rowDescription.Count; index++)
            {
                var f = _rowDescription[index];
                // Only column fields
                if (f.TableOID != 0)
                    filters.Add($"(attr.attrelid={f.TableOID} AND attr.attnum={f.ColumnAttributeNumber})");
            }

            var columnFieldFilter = string.Join(" OR ", filters);
            if (columnFieldFilter != string.Empty)
            {
                var query = oldQueryMode
                    ? GenerateOldColumnsQuery(columnFieldFilter)
                    : GenerateColumnsQuery(_connection.PostgreSqlVersion, columnFieldFilter);

                using var scope = new TransactionScope(
                    TransactionScopeOption.Suppress,
                    async ? TransactionScopeAsyncFlowOption.Enabled : TransactionScopeAsyncFlowOption.Suppress);
                using var connection = (GaussDBConnection)((ICloneable)_connection).Clone();

                await connection.Open(async, cancellationToken).ConfigureAwait(false);

                using var cmd = new GaussDBCommand(query, connection);
                var reader = await cmd.ExecuteReader(async, CommandBehavior.Default, cancellationToken).ConfigureAwait(false);
                try
                {
                    while (async ? await reader.ReadAsync(cancellationToken).ConfigureAwait(false) : reader.Read())
                    {
                        var column = LoadColumnDefinition(reader, _connection.Connector!.DatabaseInfo, oldQueryMode);
                        for (var ordinal = 0; ordinal < numFields; ordinal++)
                        {
                            var field = _rowDescription[ordinal];
                            if (field.TableOID == column.TableOID &&
                                field.ColumnAttributeNumber == column.ColumnAttributeNumber)
                            {
                                populatedColumns++;

                                if (column.ColumnOrdinal.HasValue)
                                    column = column.Clone();

                                // The column's ordinal is with respect to the resultset, not its table
                                column.ColumnOrdinal = ordinal;
                                result[ordinal] = column;
                            }
                        }
                    }
                }
                finally
                {
                    if (async)
                        await reader.DisposeAsync().ConfigureAwait(false);
                    else
                        reader.Dispose();
                }
            }
        }

        // We had some fields which don't correspond to regular table columns (or fetchAdditionalInfo is false).
        // Fill in whatever info we have from the RowDescription itself
        for (var i = 0; i < numFields; i++)
        {
            var column = result[i];
            var field = _rowDescription[i];

            if (column is null)
            {
                column = SetUpNonColumnField(field);
                column.ColumnOrdinal = i;
                result[i] = column;
                populatedColumns++;
            }

            column.ColumnName = field.Name;
            column.IsAliased = column.BaseColumnName is null ? default(bool?) : (column.BaseColumnName != column.ColumnName);
        }

        if (populatedColumns != numFields)
            throw new GaussDBException("Could not load all columns for the resultset");

        return result.AsReadOnly()!;
    }

    GaussDBDbColumn LoadColumnDefinition(GaussDBDataReader reader, GaussDBDatabaseInfo databaseInfo, bool oldQueryMode)
    {
        // We don't set ColumnName here. It should always contain the column alias rather than
        // the table column name (i.e. in case of "SELECT foo AS foo_alias"). It will be set later.
        var column = new GaussDBDbColumn
        {
            AllowDBNull = !reader.GetBoolean(reader.GetOrdinal("attnotnull")),
            BaseCatalogName = _connection.Database!,
            BaseSchemaName = reader.GetString(reader.GetOrdinal("nspname")),
            BaseServerName = _connection.Host!,
            BaseTableName = reader.GetString(reader.GetOrdinal("relname")),
            BaseColumnName = reader.GetString(reader.GetOrdinal("attname")),
            ColumnAttributeNumber = reader.GetInt16(reader.GetOrdinal("attnum")),
            IsKey = reader.GetBoolean(reader.GetOrdinal("isprimarykey")),
            IsReadOnly = !reader.GetBoolean(reader.GetOrdinal("is_updatable")),
            IsUnique = reader.GetBoolean(reader.GetOrdinal("isunique")),

            TableOID = reader.GetFieldValue<uint>(reader.GetOrdinal("attrelid")),
            TypeOID = reader.GetFieldValue<uint>(reader.GetOrdinal("typoid"))
        };

        column.PostgresType = databaseInfo.ByOID[column.TypeOID];
        column.DataTypeName = column.PostgresType.DisplayName; // Facets do not get included

        var defaultValueOrdinal = reader.GetOrdinal("default");
        column.DefaultValue = reader.IsDBNull(defaultValueOrdinal) ? null : reader.GetString(defaultValueOrdinal);

        column.IsIdentity = !oldQueryMode && reader.GetBoolean(reader.GetOrdinal("isidentity"));

        // Use a heuristic to discover old SERIAL columns
        column.IsAutoIncrement =
            column.IsIdentity == true ||
            column.DefaultValue != null && column.DefaultValue.StartsWith("nextval(", StringComparison.Ordinal);

        ColumnPostConfig(column, reader.GetInt32(reader.GetOrdinal("typmod")));

        return column;
    }

    GaussDBDbColumn SetUpNonColumnField(FieldDescription field)
    {
        // ColumnName and BaseColumnName will be set later
        var column = new GaussDBDbColumn
        {
            BaseCatalogName = _connection.Database!,
            BaseServerName = _connection.Host!,
            IsReadOnly = true,
            DataTypeName = field.PostgresType.DisplayName,
            TypeOID = field.TypeOID,
            TableOID = field.TableOID,
            ColumnAttributeNumber = field.ColumnAttributeNumber,
            PostgresType = field.PostgresType
        };

        ColumnPostConfig(column, field.TypeModifier);

        return column;
    }

    /// <summary>
    /// Performs some post-setup configuration that's common to both table columns and non-columns.
    /// </summary>
    void ColumnPostConfig(GaussDBDbColumn column, int typeModifier)
    {
        var serializerOptions = _connection.Connector!.SerializerOptions;

        column.GaussDBDbType = column.PostgresType.DataTypeName.ToGaussDBDbType();
        if (serializerOptions.GetDefaultTypeInfo(serializerOptions.ToCanonicalTypeId(column.PostgresType)) is { } typeInfo)
        {
            column.DataType = typeInfo.Type;
            column.IsLong = column.PostgresType.DataTypeName == DataTypeNames.Bytea;

            if (column.PostgresType is PostgresCompositeType)
                column.UdtAssemblyQualifiedName = typeInfo.Type.AssemblyQualifiedName;
        }

        var facets = column.PostgresType.GetFacets(typeModifier);
        if (facets.Size != null)
            column.ColumnSize = facets.Size;
        if (facets.Precision != null)
            column.NumericPrecision = facets.Precision;
        if (facets.Scale != null)
            column.NumericScale = facets.Scale;
    }
}
