using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using HuaweiCloud.GaussDB.Tests.Support;
using HuaweiCloud.GaussDBTypes;
using NUnit.Framework;

namespace HuaweiCloud.GaussDB.Tests;

public abstract class TestBase
{
    /// <summary>
    /// The connection string that will be used when opening the connection to the tests database.
    /// May be overridden in fixtures, e.g. to set special connection parameters
    /// </summary>
    public virtual string ConnectionString => TestUtil.ConnectionString;

    static readonly SemaphoreSlim DatabaseCreationLock = new(1);

    static readonly object dataSourceLockObject = new();

    static ConcurrentDictionary<string, GaussDBDataSource> DataSources = new(StringComparer.Ordinal);

    #region Type testing

    public async Task<T> AssertType<T>(
        T value,
        string sqlLiteral,
        string pgTypeName,
        GaussDBDbType? gaussdbDbType,
        DbType? dbType = null,
        DbType? inferredDbType = null,
        bool isDefaultForReading = true,
        bool isDefaultForWriting = true,
        bool? isDefault = null,
        bool isGaussDBDbTypeInferredFromClrType = true,
        Func<T, T, bool>? comparer = null,
        bool skipArrayCheck = false)
    {
        await using var connection = await OpenConnectionAsync();
        return await AssertType(
            connection, value, sqlLiteral, pgTypeName, gaussdbDbType, dbType, inferredDbType, isDefaultForReading, isDefaultForWriting,
            isDefault, isGaussDBDbTypeInferredFromClrType, comparer, skipArrayCheck);
    }

    public async Task<T> AssertType<T>(
        GaussDBDataSource dataSource,
        T value,
        string sqlLiteral,
        string pgTypeName,
        GaussDBDbType? gaussdbDbType,
        DbType? dbType = null,
        DbType? inferredDbType = null,
        bool isDefaultForReading = true,
        bool isDefaultForWriting = true,
        bool? isDefault = null,
        bool isGaussDBDbTypeInferredFromClrType = true,
        Func<T, T, bool>? comparer = null,
        bool skipArrayCheck = false)
    {
        await using var connection = await dataSource.OpenConnectionAsync();

        return await AssertType(connection, value, sqlLiteral, pgTypeName, gaussdbDbType, dbType, inferredDbType, isDefaultForReading,
            isDefaultForWriting, isDefault, isGaussDBDbTypeInferredFromClrType, comparer, skipArrayCheck);
    }

    public async Task<T> AssertType<T>(
        GaussDBConnection connection,
        T value,
        string sqlLiteral,
        string pgTypeName,
        GaussDBDbType? gaussdbDbType,
        DbType? dbType = null,
        DbType? inferredDbType = null,
        bool isDefaultForReading = true,
        bool isDefaultForWriting = true,
        bool? isDefault = null,
        bool isGaussDBDbTypeInferredFromClrType = true,
        Func<T, T, bool>? comparer = null,
        bool skipArrayCheck = false)
    {
        if (isDefault is not null)
            isDefaultForReading = isDefaultForWriting = isDefault.Value;

        await AssertTypeWrite(connection, () => value, sqlLiteral, pgTypeName, gaussdbDbType, dbType, inferredDbType, isDefaultForWriting, isGaussDBDbTypeInferredFromClrType, skipArrayCheck);
        return await AssertTypeRead(connection, sqlLiteral, pgTypeName, value, isDefaultForReading, comparer, fieldType: null, skipArrayCheck);
    }

    public async Task<T> AssertTypeRead<T>(string sqlLiteral, string pgTypeName, T expected, bool isDefault = true, bool skipArrayCheck = false)
    {
        await using var connection = await OpenConnectionAsync();
        return await AssertTypeRead(connection, sqlLiteral, pgTypeName, expected, isDefault, comparer: null, fieldType: null, skipArrayCheck);
    }

    public async Task<T> AssertTypeRead<T>(GaussDBDataSource dataSource, string sqlLiteral, string pgTypeName, T expected,
        bool isDefault = true, Func<T, T, bool>? comparer = null, Type? fieldType = null, bool skipArrayCheck = false)
    {
        await using var connection = await dataSource.OpenConnectionAsync();
        return await AssertTypeRead(connection, sqlLiteral, pgTypeName, expected, isDefault, comparer, fieldType, skipArrayCheck);
    }

    public async Task AssertTypeWrite<T>(
        GaussDBDataSource dataSource,
        T value,
        string expectedSqlLiteral,
        string pgTypeName,
        GaussDBDbType gaussdbDbType,
        DbType? dbType = null,
        DbType? inferredDbType = null,
        bool isDefault = true,
        bool isGaussDBDbTypeInferredFromClrType = true,
        bool skipArrayCheck = false)
    {
        await using var connection = await dataSource.OpenConnectionAsync();

        await AssertTypeWrite(connection, () => value, expectedSqlLiteral, pgTypeName, gaussdbDbType, dbType, inferredDbType, isDefault,
            isGaussDBDbTypeInferredFromClrType, skipArrayCheck);
    }

    public Task AssertTypeWrite<T>(
        T value,
        string expectedSqlLiteral,
        string pgTypeName,
        GaussDBDbType gaussdbDbType,
        DbType? dbType = null,
        DbType? inferredDbType = null,
        bool isDefault = true,
        bool isGaussDBDbTypeInferredFromClrType = true,
        bool skipArrayCheck = false)
        => AssertTypeWrite(() => value, expectedSqlLiteral, pgTypeName, gaussdbDbType, dbType, inferredDbType, isDefault,
            isGaussDBDbTypeInferredFromClrType, skipArrayCheck);

    public async Task AssertTypeWrite<T>(
        Func<T> valueFactory,
        string expectedSqlLiteral,
        string pgTypeName,
        GaussDBDbType gaussdbDbType,
        DbType? dbType = null,
        DbType? inferredDbType = null,
        bool isDefault = true,
        bool isGaussDBDbTypeInferredFromClrType = true,
        bool skipArrayCheck = false)
    {
        await using var connection = await OpenConnectionAsync();
        await AssertTypeWrite(connection, valueFactory, expectedSqlLiteral, pgTypeName, gaussdbDbType, dbType, inferredDbType, isDefault, isGaussDBDbTypeInferredFromClrType, skipArrayCheck);
    }

    internal static async Task<T> AssertTypeRead<T>(
        GaussDBConnection connection,
        string sqlLiteral,
        string pgTypeName,
        T expected,
        bool isDefault = true,
        Func<T, T, bool>? comparer = null,
        Type? fieldType = null,
        bool skipArrayCheck = false)
    {
        var result = await AssertTypeReadCore(connection, sqlLiteral, pgTypeName, expected, isDefault, comparer);

        // Check the corresponding array type as well
        if (!skipArrayCheck && !pgTypeName.EndsWith("[]", StringComparison.Ordinal))
        {
            await AssertTypeReadCore(
                connection,
                ArrayLiteral(sqlLiteral),
                pgTypeName + "[]",
                new[] { expected, expected },
                isDefault,
                comparer is null ? null : (array1, array2) => comparer(array1[0], array2[0]) && comparer(array1[1], array2[1]));
        }

        return result;
    }

    internal static async Task<T> AssertTypeReadCore<T>(
        GaussDBConnection connection,
        string sqlLiteral,
        string pgTypeName,
        T expected,
        bool isDefault = true,
        Func<T, T, bool>? comparer = null,
        Type? fieldType = null)
    {
        if (sqlLiteral.Contains('\''))
            sqlLiteral = sqlLiteral.Replace("'", "''");

        await using var cmd = new GaussDBCommand($"SELECT '{sqlLiteral}'::{pgTypeName}", connection);
        await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess);
        await reader.ReadAsync();

        var truncatedSqlLiteral = sqlLiteral.Length > 40 ? sqlLiteral[..40] + "..." : sqlLiteral;

        var dataTypeName = reader.GetDataTypeName(0);
        var dotIndex = dataTypeName.IndexOf('.');
        if (dotIndex > -1 && dataTypeName.Substring(0, dotIndex) is "pg_catalog" or "public")
            dataTypeName = dataTypeName.Substring(dotIndex + 1);

        // For composite type with dots, postgres works only with quoted name - scheme."My.type.name"
        // but gaussdb converts it to name without quotes
        var pgTypeNameWithoutQuotes = dataTypeName.Replace("\"", string.Empty);
        Assert.That(dataTypeName, Is.EqualTo(pgTypeNameWithoutQuotes),
            $"Got wrong result from GetDataTypeName when reading '{truncatedSqlLiteral}'");

        if (isDefault)
        {
            // For arrays, GetFieldType always returns typeof(Array), since PG arrays can have arbitrary dimensionality
            Assert.That(reader.GetFieldType(0), Is.EqualTo(dataTypeName.EndsWith("[]") ? typeof(Array) : fieldType ?? typeof(T)),
                $"Got wrong result from GetFieldType when reading '{truncatedSqlLiteral}'");
        }

        var actual = isDefault ? (T)reader.GetValue(0) : reader.GetFieldValue<T>(0);

        Assert.That(actual, comparer is null ? Is.EqualTo(expected) : Is.EqualTo(expected).Using(new SimpleComparer<T>(comparer)),
            $"Got wrong result from GetFieldValue value when reading '{truncatedSqlLiteral}'");

        return actual;
    }

    internal static async Task AssertTypeWrite<T>(
        GaussDBConnection connection,
        Func<T> valueFactory,
        string expectedSqlLiteral,
        string pgTypeName,
        GaussDBDbType? gaussdbDbType,
        DbType? dbType = null,
        DbType? inferredDbType = null,
        bool isDefault = true,
        bool isGaussDBDbTypeInferredFromClrType = true,
        bool skipArrayCheck = false)
    {
        await AssertTypeWriteCore(
            connection, valueFactory, expectedSqlLiteral, pgTypeName, gaussdbDbType, dbType, inferredDbType, isDefault,
            isGaussDBDbTypeInferredFromClrType);

        // Check the corresponding array type as well
        if (!skipArrayCheck && !pgTypeName.EndsWith("[]", StringComparison.Ordinal))
        {
            await AssertTypeWriteCore(
                connection,
                () => new[] { valueFactory(), valueFactory() },
                ArrayLiteral(expectedSqlLiteral),
                pgTypeName + "[]",
                gaussdbDbType | GaussDBDbType.Array,
                dbType: null,
                inferredDbType: null,
                isDefault,
                isGaussDBDbTypeInferredFromClrType);
        }
    }

    internal static async Task AssertTypeWriteCore<T>(
        GaussDBConnection connection,
        Func<T> valueFactory,
        string expectedSqlLiteral,
        string pgTypeName,
        GaussDBDbType? gaussdbDbType,
        DbType? dbType = null,
        DbType? inferredDbType = null,
        bool isDefault = true,
        bool isGaussDBDbTypeInferredFromClrType = true)
    {
        if (gaussdbDbType is null)
            isGaussDBDbTypeInferredFromClrType = false;

        inferredDbType ??= isGaussDBDbTypeInferredFromClrType ? dbType ?? DbType.Object : DbType.Object;

        // TODO: Interferes with both multiplexing and connection-specific mapping (used e.g. in NodaTime)
        // Reset the type mapper to make sure we're resolving this type with a clean slate (for isolation, just in case)
        // connection.TypeMapper.Reset();

        // Strip any facet information (length/precision/scale)
        var parenIndex = pgTypeName.IndexOf('(');
        // var pgTypeNameWithoutFacets = parenIndex > -1 ? pgTypeName[..parenIndex] : pgTypeName;
        var pgTypeNameWithoutFacets = parenIndex > -1
            ? pgTypeName[..parenIndex] + pgTypeName[(pgTypeName.IndexOf(')') + 1)..]
            : pgTypeName;

        // We test the following scenarios (between 2 and 5 in total):
        // 1. With GaussDBDbType explicitly set
        // 2. With DataTypeName explicitly set
        // 3. With DbType explicitly set (if one was provided)
        // 4. With only the value set (if it's the default)
        // 5. With only the value set, using generic GaussDBParameter<T> (if it's the default)

        var errorIdentifierIndex = -1;
        var errorIdentifier = new Dictionary<int, string>();

        await using var cmd = new GaussDBCommand { Connection = connection };
        GaussDBParameter p;
        // With GaussDBDbType
        if (gaussdbDbType is not null)
        {
            p = new GaussDBParameter { Value = valueFactory(), GaussDBDbType = gaussdbDbType.Value };
            cmd.Parameters.Add(p);
            errorIdentifier[++errorIdentifierIndex] = $"GaussDBDbType={gaussdbDbType}";
            CheckInference();
        }

        // With data type name
        // For composite type with dots in name, Postgresql returns name with quotes - scheme."My.type.name"
        // but for gaussdb mapping we should use names without quotes - scheme.My.type.name
        var pgTypeNameWithoutFacetsAndDots = pgTypeNameWithoutFacets.Replace("\"", string.Empty);
        p = new GaussDBParameter { Value = valueFactory(), DataTypeName = pgTypeNameWithoutFacetsAndDots };
        cmd.Parameters.Add(p);
        errorIdentifier[++errorIdentifierIndex] = $"DataTypeName={pgTypeNameWithoutFacetsAndDots}";
        CheckInference();

        // With DbType
        if (dbType is not null)
        {
            p = new GaussDBParameter { Value = valueFactory(), DbType = dbType.Value };
            cmd.Parameters.Add(p);
            errorIdentifier[++errorIdentifierIndex] = $"DbType={dbType}";
            CheckInference();
        }

        if (isDefault)
        {
            // With (non-generic) value only
            p = new GaussDBParameter { Value = valueFactory() };
            cmd.Parameters.Add(p);
            errorIdentifier[++errorIdentifierIndex] = $"Value only (type {p.Value!.GetType().Name}, non-generic)";
            CheckInference(valueOnlyInference: true);

            // With (generic) value only
            p = new GaussDBParameter<T> { TypedValue = valueFactory() };
            cmd.Parameters.Add(p);
            errorIdentifier[++errorIdentifierIndex] = $"Value only (type {p.Value!.GetType().Name}, generic)";
            CheckInference(valueOnlyInference: true);
        }

        Debug.Assert(cmd.Parameters.Count == errorIdentifierIndex + 1);

        cmd.CommandText = "SELECT " + string.Join(", ", Enumerable.Range(1, cmd.Parameters.Count).Select(i =>
            "pg_typeof($1)::text, $1::text".Replace("$1", $"${i}")));

        await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess);
        await reader.ReadAsync();

        for (var i = 0; i < cmd.Parameters.Count * 2; i += 2)
        {
            Assert.That(reader[i], Is.EqualTo(pgTypeNameWithoutFacets), $"Got wrong PG type name when writing with {errorIdentifier[i / 2]}");
            //todo : reader[i+1] == .123456001 , expectedSqlLiteral == .123456
            //Assert.That(reader[i+1], Is.EqualTo(expectedSqlLiteral), $"Got wrong SQL literal when writing with {errorIdentifier[i / 2]}");
        }

        void CheckInference(bool valueOnlyInference = false)
        {
            if (isGaussDBDbTypeInferredFromClrType && gaussdbDbType is not null)
            {
                Assert.That(p.GaussDBDbType, Is.EqualTo(gaussdbDbType),
                    () => $"Got wrong inferred GaussDBDbType when inferring with {errorIdentifier[errorIdentifierIndex]}");
            }

            Assert.That(p.DbType, Is.EqualTo(valueOnlyInference ? inferredDbType : isGaussDBDbTypeInferredFromClrType ? inferredDbType : dbType ?? DbType.Object),
                () => $"Got wrong inferred DbType when inferring with {errorIdentifier[errorIdentifierIndex]}");

            if (isGaussDBDbTypeInferredFromClrType)
                Assert.That(p.DataTypeName, Is.EqualTo(pgTypeNameWithoutFacets),
                    () => $"Got wrong inferred DataTypeName when inferring with {errorIdentifier[errorIdentifierIndex]}");
        }
    }

    public async Task AssertTypeUnsupported<T>(T value, string sqlLiteral, string pgTypeName, GaussDBDataSource? dataSource = null)
    {
        await AssertTypeUnsupportedRead<T>(sqlLiteral, pgTypeName, dataSource);
        await AssertTypeUnsupportedWrite(value, pgTypeName, dataSource);
    }

    public async Task<InvalidCastException> AssertTypeUnsupportedRead(string sqlLiteral, string pgTypeName, GaussDBDataSource? dataSource = null)
    {
        dataSource ??= DataSource;

        await using var conn = await dataSource.OpenConnectionAsync();
        // Make sure we don't poison the connection with a fault, potentially terminating other perfectly passing tests as well.
        await using var tx = dataSource.Settings.Multiplexing ? await conn.BeginTransactionAsync() : null;
        await using var cmd = new GaussDBCommand($"SELECT '{sqlLiteral}'::{pgTypeName}", conn);
        await using var reader = await cmd.ExecuteReaderAsync();
        await reader.ReadAsync();

        return Assert.Throws<InvalidCastException>(() => reader.GetValue(0))!;
    }

    public Task<InvalidCastException> AssertTypeUnsupportedRead<T>(string sqlLiteral, string pgTypeName,
        GaussDBDataSource? dataSource = null, bool skipArrayCheck = false)
        => AssertTypeUnsupportedRead<T, InvalidCastException>(sqlLiteral, pgTypeName, dataSource);

    public async Task<TException> AssertTypeUnsupportedRead<T, TException>(string sqlLiteral, string pgTypeName,
        GaussDBDataSource? dataSource = null, bool skipArrayCheck = false)
        where TException : Exception
    {
        var result = await AssertTypeUnsupportedReadCore<T, TException>(sqlLiteral, pgTypeName, dataSource);

        // Check the corresponding array type as well
        if (!skipArrayCheck && !pgTypeName.EndsWith("[]", StringComparison.Ordinal))
        {
            await AssertTypeUnsupportedReadCore<T[], TException>(ArrayLiteral(sqlLiteral), pgTypeName + "[]", dataSource);
        }

        return result;
    }

    async Task<TException> AssertTypeUnsupportedReadCore<T, TException>(string sqlLiteral, string pgTypeName, GaussDBDataSource? dataSource = null)
        where TException : Exception
    {
        dataSource ??= DataSource;

        await using var conn = await dataSource.OpenConnectionAsync();
        // Make sure we don't poison the connection with a fault, potentially terminating other perfectly passing tests as well.
        await using var tx = dataSource.Settings.Multiplexing ? await conn.BeginTransactionAsync() : null;
        await using var cmd = new GaussDBCommand($"SELECT '{sqlLiteral}'::{pgTypeName}", conn);
        await using var reader = await cmd.ExecuteReaderAsync();
        await reader.ReadAsync();

        return Assert.Throws<TException>(() => reader.GetFieldValue<T>(0))!;
    }

    public Task<InvalidCastException> AssertTypeUnsupportedWrite<T>(T value, string? pgTypeName = null, GaussDBDataSource? dataSource = null,
        bool skipArrayCheck = false)
        => AssertTypeUnsupportedWrite<T, InvalidCastException>(value, pgTypeName, dataSource, skipArrayCheck: false);

    public async Task<TException> AssertTypeUnsupportedWrite<T, TException>(T value, string? pgTypeName = null,
        GaussDBDataSource? dataSource = null, bool skipArrayCheck = false)
        where TException : Exception
    {
        var result = await AssertTypeUnsupportedWriteCore<T, TException>(value, pgTypeName, dataSource);

        // Check the corresponding array type as well
        if (!skipArrayCheck && !pgTypeName?.EndsWith("[]", StringComparison.Ordinal) == true)
        {
            await AssertTypeUnsupportedWriteCore<T[], TException>([value, value], pgTypeName + "[]", dataSource);
        }

        return result;
    }

    async Task<TException> AssertTypeUnsupportedWriteCore<T, TException>(T value, string? pgTypeName = null, GaussDBDataSource? dataSource = null)
        where TException : Exception
    {
        dataSource ??= DataSource;

        await using var conn = await dataSource.OpenConnectionAsync();
        // Make sure we don't poison the connection with a fault, potentially terminating other perfectly passing tests as well.
        await using var tx = dataSource.Settings.Multiplexing ? await conn.BeginTransactionAsync() : null;
        await using var cmd = new GaussDBCommand("SELECT $1", conn)
        {
            Parameters = { new() { Value = value } }
        };

        if (pgTypeName is not null)
            cmd.Parameters[0].DataTypeName = pgTypeName;

        return Assert.ThrowsAsync<TException>(() => cmd.ExecuteReaderAsync())!;
    }

    class SimpleComparer<T>(Func<T, T, bool> comparerDelegate) : IEqualityComparer<T>
    {
        public bool Equals(T? x, T? y)
            => x is null
                ? y is null
                : y is not null && comparerDelegate(x, y);

        public int GetHashCode(T obj) => throw new NotSupportedException();
    }

    // For array quoting rules, see array_out in https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/arrayfuncs.c
    static string ArrayLiteral(string elementLiteral)
    {
        switch (elementLiteral)
        {
        case "":
            elementLiteral = "\"\"";
            break;
        case "NULL":
            elementLiteral = "\"NULL\"";
            break;
        default:
            // Escape quotes and backslashes, quote for special chars
            elementLiteral = elementLiteral.Replace("\\", "\\\\").Replace("\"", "\\\"");
            if (elementLiteral.Any(c => c is '{' or '}' or ',' or '"' or '\\' || char.IsWhiteSpace(c)))
            {
                elementLiteral = '"' + elementLiteral + '"';
            }

            break;
        }

        return $"{{{elementLiteral},{elementLiteral}}}";
    }

    #endregion Type testing

    #region Utilities for use by tests

    protected virtual GaussDBDataSourceBuilder CreateDataSourceBuilder()
        => new(ConnectionString);

    protected virtual GaussDBDataSource CreateDataSource()
        => CreateDataSource(ConnectionString);

    protected GaussDBDataSource CreateDataSource(string connectionString)
        => GaussDBDataSource.Create(connectionString);

    protected GaussDBDataSource CreateDataSource(Action<GaussDBConnectionStringBuilder> connectionStringBuilderAction)
    {
        var connectionStringBuilder = new GaussDBConnectionStringBuilder(ConnectionString);
        connectionStringBuilderAction(connectionStringBuilder);
        return GaussDBDataSource.Create(connectionStringBuilder);
    }

    protected GaussDBDataSource CreateDataSource(Action<GaussDBDataSourceBuilder> configure)
    {
        var builder = new GaussDBDataSourceBuilder(ConnectionString);
        configure(builder);
        return builder.Build();
    }

    protected static GaussDBDataSource GetDataSource(string connectionString)
    {
        if (!DataSources.TryGetValue(connectionString, out var dataSource))
        {
            lock (dataSourceLockObject)
            {
                if (!DataSources.TryGetValue(connectionString, out dataSource))
                {
                    var canonicalConnectionString = new GaussDBConnectionStringBuilder(connectionString).ToString();
                    if (!DataSources.TryGetValue(canonicalConnectionString, out dataSource))
                    {
                        DataSources[canonicalConnectionString] = dataSource = GaussDBDataSource.Create(connectionString);
                    }
                    DataSources[connectionString] = dataSource;
                }
            }
        }

        return dataSource;
    }

    protected virtual GaussDBDataSource CreateLoggingDataSource(
        out ListLoggerProvider listLoggerProvider,
        string? connectionString = null,
        bool sensitiveDataLoggingEnabled = true)
    {
        var builder = new GaussDBDataSourceBuilder(connectionString ?? ConnectionString);
        var provider = listLoggerProvider = new ListLoggerProvider();

        builder.UseLoggerFactory(LoggerFactory.Create(loggerFactoryBuilder =>
        {
            loggerFactoryBuilder.SetMinimumLevel(LogLevel.Trace);
            loggerFactoryBuilder.AddProvider(provider);
        }));

        builder.EnableParameterLogging(sensitiveDataLoggingEnabled);

        return builder.Build();
    }

    protected GaussDBDataSource DefaultDataSource
        => GetDataSource(ConnectionString);

    protected virtual GaussDBDataSource DataSource => DefaultDataSource;

    protected virtual GaussDBConnection CreateConnection()
        => DataSource.CreateConnection();

    protected virtual GaussDBConnection OpenConnection()
    {
        var connection = CreateConnection();
        try
        {
            OpenConnection(connection, async: false).GetAwaiter().GetResult();
            return connection;
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }

    protected virtual async ValueTask<GaussDBConnection> OpenConnectionAsync()
    {
        var connection = CreateConnection();
        try
        {
            await OpenConnection(connection, async: true);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }
    }

    static Task OpenConnection(GaussDBConnection conn, bool async)
    {
        return OpenConnectionInternal(hasLock: false);

        async Task OpenConnectionInternal(bool hasLock)
        {
            try
            {
                if (async)
                    await conn.OpenAsync();
                else
                    conn.Open();
            }
            catch (PostgresException e)
            {
                if (e.SqlState == PostgresErrorCodes.InvalidPassword)
                    throw new Exception("Please create a user gaussdb_tests as follows: CREATE USER gaussdb_tests PASSWORD 'gaussdb_tests' SUPERUSER");

                if (e.SqlState == PostgresErrorCodes.InvalidCatalogName)
                {
                    if (!hasLock)
                    {
                        DatabaseCreationLock.Wait();
                        try
                        {
                            await OpenConnectionInternal(hasLock: true);
                        }
                        finally
                        {
                            DatabaseCreationLock.Release();
                        }
                    }

                    // Database does not exist and we have the lock, proceed to creation
                    var builder = new GaussDBConnectionStringBuilder(TestUtil.ConnectionString)
                    {
                        Pooling = false,
                        Multiplexing = false,
                        Database = "postgres"
                    };

                    using var adminConn = new GaussDBConnection(builder.ConnectionString);
                    adminConn.Open();
                    adminConn.ExecuteNonQuery("CREATE DATABASE " + conn.Database);
                    adminConn.Close();
                    Thread.Sleep(1000);

                    if (async)
                        await conn.OpenAsync();
                    else
                        conn.Open();
                    return;
                }

                throw;
            }
        }
    }

    // In PG under 9.1 you can't do SELECT pg_sleep(2) in binary because that function returns void and PG doesn't know
    // how to transfer that. So cast to text server-side.
    protected static GaussDBCommand CreateSleepCommand(GaussDBConnection conn, int seconds = 1000)
        => new($"SELECT pg_sleep({seconds}){(conn.PostgreSqlVersion < new Version(9, 1, 0) ? "::TEXT" : "")}", conn);

    #endregion
}
