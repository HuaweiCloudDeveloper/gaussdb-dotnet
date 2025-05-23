using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace HuaweiCloud.GaussDB.Tests;

public static class TestUtil
{
    /// <summary>
    /// Unless the NPGSQL_TEST_DB environment variable is defined, this is used as the connection string for the
    /// test database.
    /// </summary>
    public const string DefaultConnectionString =
        "Host=localhost;Port=8000;Username=gaussdb;Password=Password@123;Database=postgres;Timeout=0;Command Timeout=0;SSL Mode=Disable;Multiplexing=False";

    /// <summary>
    /// The connection string that will be used when opening the connection to the tests database.
    /// May be overridden in fixtures, e.g. to set special connection parameters
    /// </summary>
    public static string ConnectionString { get; }
        = Environment.GetEnvironmentVariable("NPGSQL_TEST_DB") ?? DefaultConnectionString;

    public static bool IsOnBuildServer =>
        Environment.GetEnvironmentVariable("GITHUB_ACTIONS") != null ||
        Environment.GetEnvironmentVariable("CI") != null;

    /// <summary>
    /// Calls Assert.Ignore() unless we're on the build server, in which case calls
    /// Assert.Fail(). We don't to miss any regressions just because something was misconfigured
    /// at the build server and caused a test to be inconclusive.
    /// </summary>
    [DoesNotReturn]
    public static void IgnoreExceptOnBuildServer(string message)
    {
        if (IsOnBuildServer)
            Assert.Fail(message);
        else
            Assert.Ignore(message);

        throw new Exception("Should not occur");
    }

    public static void IgnoreExceptOnBuildServer(string message, params object[] args)
        => IgnoreExceptOnBuildServer(string.Format(message, args));

    public static void MinimumPgVersion(GaussDBDataSource dataSource, string minVersion, string? ignoreText = null)
    {
        using var connection = dataSource.OpenConnection();
        MinimumPgVersion(connection, minVersion, ignoreText);
    }

    public static bool MinimumPgVersion(GaussDBConnection conn, string minVersion, string? ignoreText = null)
    {
        var min = new Version(minVersion);
        if (conn.PostgreSqlVersion < min)
        {
            var msg = $"Postgresql backend version {conn.PostgreSqlVersion} is less than the required {min}";
            if (ignoreText != null)
                msg += ": " + ignoreText;
            Assert.Ignore(msg);
            return false;
        }

        return true;
    }

    public static void MaximumPgVersionExclusive(GaussDBConnection conn, string maxVersion, string? ignoreText = null)
    {
        var max = new Version(maxVersion);
        if (conn.PostgreSqlVersion >= max)
        {
            var msg = $"Postgresql backend version {conn.PostgreSqlVersion} is greater than or equal to the required (exclusive) maximum of {maxVersion}";
            if (ignoreText != null)
                msg += ": " + ignoreText;
            Assert.Ignore(msg);
        }
    }

    static readonly Version MinCreateExtensionVersion = new(9, 1);

    public static async Task IgnoreOnRedshift(GaussDBConnection conn, string? ignoreText = null)
    {
        await using var command = conn.CreateCommand();
        command.CommandText = "SELECT version()";
        var version = (string)(await command.ExecuteScalarAsync())!;
        if (version.Contains("redshift", StringComparison.OrdinalIgnoreCase))
        {
            var msg = "Test ignored on Redshift";
            if (ignoreText != null)
                msg += ": " + ignoreText;
            Assert.Ignore(msg);
        }
    }

    public static async Task<bool> IsPgPrerelease(GaussDBConnection conn)
        => ((string) (await conn.ExecuteScalarAsync("SELECT version()"))!).Contains("beta");

    public static void EnsureExtension(GaussDBConnection conn, string extension, string? minVersion = null)
        => EnsureExtension(conn, extension, minVersion, async: false).GetAwaiter().GetResult();

    public static Task EnsureExtensionAsync(GaussDBConnection conn, string extension, string? minVersion = null)
        => EnsureExtension(conn, extension, minVersion, async: true);

    static async Task EnsureExtension(GaussDBConnection conn, string extension, string? minVersion, bool async)
    {
        if (minVersion != null && !MinimumPgVersion(conn, minVersion, $"The extension '{extension}' only works for PostgreSQL {minVersion} and higher."))
            return;

        if (conn.PostgreSqlVersion < MinCreateExtensionVersion)
            Assert.Ignore($"The 'CREATE EXTENSION' command only works for PostgreSQL {MinCreateExtensionVersion} and higher.");

        try
        {
            if (async)
                await conn.ExecuteNonQueryAsync($"CREATE EXTENSION IF NOT EXISTS {extension}");
            else
                conn.ExecuteNonQuery($"CREATE EXTENSION IF NOT EXISTS {extension}");
        }
        catch (PostgresException ex) when (ex.ConstraintName == "pg_extension_name_index")
        {
            // The extension is already installed, but we can race across threads.
            // https://stackoverflow.com/questions/63104126/create-extention-if-not-exists-doesnt-really-check-if-extention-does-not-exis
        }

        conn.ReloadTypes();
    }

    /// <summary>
    /// Causes the test to be ignored if the supplied query fails with SqlState 0A000 (feature_not_supported)
    /// </summary>
    /// <param name="conn">The connection to execute the test query. The connection needs to be open.</param>
    /// <param name="testQuery">The query to test for the feature.
    /// This query needs to fail with SqlState 0A000 (feature_not_supported) if the feature isn't present.</param>
    public static void IgnoreIfFeatureNotSupported(GaussDBConnection conn, string testQuery)
        => IgnoreIfFeatureNotSupported(conn, testQuery, async: false).GetAwaiter().GetResult();

    /// <summary>
    /// Causes the test to be ignored if the supplied query fails with SqlState 0A000 (feature_not_supported)
    /// </summary>
    /// <param name="conn">The connection to execute the test query. The connection needs to be open.</param>
    /// <param name="testQuery">The query to test for the feature.
    /// This query needs to fail with SqlState 0A000 (feature_not_supported) if the feature isn't present.</param>
    public static Task IgnoreIfFeatureNotSupportedAsync(GaussDBConnection conn, string testQuery)
        => IgnoreIfFeatureNotSupported(conn, testQuery, async: true);

    static async Task IgnoreIfFeatureNotSupported(GaussDBConnection conn, string testQuery, bool async)
    {
        try
        {
            if (async)
                await conn.ExecuteNonQueryAsync(testQuery);
            else
                conn.ExecuteNonQuery(testQuery);
        }
        catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.FeatureNotSupported)
        {
            Assert.Ignore(e.Message);
        }
    }

    public static async Task EnsurePostgis(GaussDBConnection conn)
    {
        var isPreRelease = await IsPgPrerelease(conn);
        try
        {
            await EnsureExtensionAsync(conn, "postgis");
        }
        catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.UndefinedFile)
        {
            // PostGIS packages aren't available for PostgreSQL prereleases
            if (isPreRelease)
            {
                Assert.Ignore($"PostGIS could not be installed, but PostgreSQL is prerelease ({conn.ServerVersion}), ignoring test suite.");
            }
            else
            {
                throw;
            }
        }
    }

    public static string GetUniqueIdentifier(string prefix)
        => prefix + Interlocked.Increment(ref _counter);

    static int _counter;

    /// <summary>
    /// Creates a table with a unique name, usable for a single test.
    /// </summary>
    internal static async Task<string> CreateTempTable(GaussDBConnection conn, string columns)
    {
        var tableName = "temp_table" + Interlocked.Increment(ref _tempTableCounter);

        await conn.ExecuteNonQueryAsync(@$"
START TRANSACTION;
SELECT pg_advisory_xact_lock(0);
DROP TABLE IF EXISTS {tableName} CASCADE;
COMMIT;
CREATE TABLE {tableName} ({columns});");
        return tableName;
    }

    /// <summary>
    /// Generates a unique table name, usable for a single test, and drops it if it already exists.
    /// Actual creation of the table is the responsibility of the caller.
    /// </summary>
    internal static async Task<string> GetTempTableName(GaussDBConnection conn)
    {
        var tableName = "temp_table" + Interlocked.Increment(ref _tempTableCounter);
        await conn.ExecuteNonQueryAsync(@$"
START TRANSACTION;
SELECT pg_advisory_xact_lock(0);
DROP TABLE IF EXISTS {tableName} CASCADE;
COMMIT");
        return tableName;
    }

    /// <summary>
    /// Creates a table with a unique name, usable for a single test, and returns an <see cref="IDisposable"/> to
    /// drop it at the end of the test.
    /// </summary>
    internal static async Task<string> CreateTempTable(GaussDBDataSource dataSource, string columns)
    {
        var tableName = "temp_table" + Interlocked.Increment(ref _tempTableCounter);
        await dataSource.ExecuteNonQueryAsync(@$"
START TRANSACTION;
SELECT pg_advisory_xact_lock(0);
DROP TABLE IF EXISTS {tableName} CASCADE;
COMMIT;
CREATE TABLE {tableName} ({columns});");
        return tableName;
    }

    /// <summary>
    /// Creates a schema with a unique name, usable for a single test.
    /// </summary>
    internal static async Task<string> CreateTempSchema(GaussDBConnection conn)
    {
        var schemaName = "temp_schema" + Interlocked.Increment(ref _tempSchemaCounter);
        await conn.ExecuteNonQueryAsync($"DROP SCHEMA IF EXISTS {schemaName} ; CREATE SCHEMA {schemaName}");
        return schemaName;
    }

    /// <summary>
    /// Generates a unique view name, usable for a single test, and drops it if it already exists.
    /// Actual creation of the view is the responsibility of the caller.
    /// </summary>
    internal static async Task<string> GetTempViewName(GaussDBConnection conn)
    {
        var viewName = "temp_view" + Interlocked.Increment(ref _tempViewCounter);
        await conn.ExecuteNonQueryAsync($"DROP VIEW IF EXISTS {viewName} ");
        return viewName;
    }

    /// <summary>
    /// Generates a unique materialized view name, usable for a single test, and drops it if it already exists.
    /// Actual creation of the materialized view is the responsibility of the caller.
    /// </summary>
    internal static async Task<string> GetTempMaterializedViewName(GaussDBConnection conn)
    {
        var viewName = "temp_materialized_view" + Interlocked.Increment(ref _tempViewCounter);
        await conn.ExecuteNonQueryAsync($"DROP MATERIALIZED VIEW IF EXISTS {viewName} ");
        return viewName;
    }

    /// <summary>
    /// Generates a unique function name, usable for a single test.
    /// Actual creation of the function is the responsibility of the caller.
    /// </summary>
    internal static async Task<string> GetTempFunctionName(GaussDBConnection conn)
    {
        var functionName = "temp_func" + Interlocked.Increment(ref _tempFunctionCounter);
        await conn.ExecuteNonQueryAsync($"DROP FUNCTION IF EXISTS {functionName}");
        return functionName;
    }

    /// <summary>
    /// Generates a unique function name, usable for a single test.
    /// Actual creation of the function is the responsibility of the caller.
    /// </summary>
    /// <returns>
    /// An <see cref="IDisposable"/> to drop the function at the end of the test.
    /// </returns>
    internal static async Task<string> GetTempProcedureName(GaussDBDataSource dataSource)
    {
        var procedureName = "temp_procedure" + Interlocked.Increment(ref _tempProcedureCounter);
        await dataSource.ExecuteNonQueryAsync($"DROP PROCEDURE IF EXISTS {procedureName} ");
        return procedureName;
    }

    /// <summary>
    /// Generates a unique function name, usable for a single test.
    /// Actual creation of the function is the responsibility of the caller.
    /// </summary>
    /// <returns>
    /// An <see cref="IDisposable"/> to drop the function at the end of the test.
    /// </returns>
    internal static async Task<string> GetTempProcedureName(GaussDBConnection connection)
    {
        var procedureName = "temp_procedure" + Interlocked.Increment(ref _tempProcedureCounter);
        await connection.ExecuteNonQueryAsync($"DROP PROCEDURE IF EXISTS {procedureName} ");
        return procedureName;
    }

    /// <summary>
    /// Generates a unique type name, usable for a single test.
    /// Actual creation of the type is the responsibility of the caller.
    /// </summary>
    internal static async Task<string> GetTempTypeName(GaussDBConnection conn)
    {
        var typeName = "temp_type" + Interlocked.Increment(ref _tempTypeCounter);
        await conn.ExecuteNonQueryAsync($"DROP TYPE IF EXISTS {typeName} CASCADE");
        return typeName;
    }

    internal static volatile int _tempTableCounter;
    static volatile int _tempViewCounter;
    static volatile int _tempFunctionCounter;
    static volatile int _tempProcedureCounter;
    static volatile int _tempSchemaCounter;
    static volatile int _tempTypeCounter;

    /// <summary>
    /// Creates a pool with a unique application name, usable for a single test, and returns an
    /// <see cref="IDisposable"/> to drop it at the end of the test.
    /// </summary>
    internal static IDisposable CreateTempPool(string origConnectionString, out string tempConnectionString)
        => CreateTempPool(new GaussDBConnectionStringBuilder(origConnectionString), out tempConnectionString);

    /// <summary>
    /// Creates a pool with a unique application name, usable for a single test, and returns an
    /// <see cref="IDisposable"/> to drop it at the end of the test.
    /// </summary>
    internal static IDisposable CreateTempPool(GaussDBConnectionStringBuilder builder, out string tempConnectionString)
    {
        builder.ApplicationName = (builder.ApplicationName ?? "TempPool") + Interlocked.Increment(ref _tempPoolCounter);
        tempConnectionString = builder.ConnectionString;
        return new PoolDisposer(tempConnectionString);
    }

    static volatile int _tempPoolCounter;

    readonly struct PoolDisposer : IDisposable
    {
        readonly string _connectionString;

        internal PoolDisposer(string connectionString) => _connectionString = connectionString;

        public void Dispose()
        {
            var conn = new GaussDBConnection(_connectionString);
            GaussDBConnection.ClearPool(conn);
        }
    }

    /// <summary>
    /// Utility to generate a bytea literal in Postgresql hex format
    /// See https://www.postgresql.org/docs/current/static/datatype-binary.html
    /// </summary>
    internal static string EncodeByteaHex(ICollection<byte> buf)
    {
        var hex = new StringBuilder(@"E'\\x", buf.Count * 2 + 3);
        foreach (var b in buf)
            hex.Append($"{b:x2}");
        hex.Append("'");
        return hex.ToString();
    }

    internal static IDisposable SetEnvironmentVariable(string name, string? value)
    {
        var oldValue = Environment.GetEnvironmentVariable(name);
        Environment.SetEnvironmentVariable(name, value);
        return new DeferredExecutionDisposable(() => Environment.SetEnvironmentVariable(name, oldValue));
    }

    internal static IDisposable SetCurrentCulture(CultureInfo culture)
    {
        var oldCulture = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = culture;

        return new DeferredExecutionDisposable(() => CultureInfo.CurrentCulture = oldCulture);
    }

    internal static IDisposable DisableSqlRewriting()
    {
#if DEBUG
        GaussDBCommand.EnableSqlRewriting = false;
        return new DeferredExecutionDisposable(() => GaussDBCommand.EnableSqlRewriting = true);
#else
        Assert.Ignore("Cannot disable SQL rewriting in RELEASE builds");
        throw new NotSupportedException("Cannot disable SQL rewriting in RELEASE builds");
#endif
    }

    class DeferredExecutionDisposable : IDisposable
    {
        readonly Action _action;

        internal DeferredExecutionDisposable(Action action) => _action = action;

        public void Dispose()
            => _action();
    }

    internal static object AssertLoggingStateContains(
        (LogLevel Level, EventId Id, string Message, object? State, Exception? Exception) log,
        string key)
    {
        if (log.State is not IEnumerable<KeyValuePair<string, object>> keyValuePairs || keyValuePairs.All(kvp => kvp.Key != key))
        {
            Assert.Fail($@"Dod not find logging state key ""{key}""");
            throw new Exception();
        }

        return keyValuePairs.Single(kvp => kvp.Key == key).Value;
    }

    internal static void AssertLoggingStateContains<T>(
        (LogLevel Level, EventId Id, string Message, object? State, Exception? Exception) log,
        string key,
        T value)
        => Assert.That(log.State, Contains.Item(new KeyValuePair<string, T>(key, value)));

    internal static void AssertLoggingStateDoesNotContain(
        (LogLevel Level, EventId Id, string Message, object? State, Exception? Exception) log,
        string key)
    {
        var value = log.State is IEnumerable<KeyValuePair<string, object>> keyValuePairs &&
                    keyValuePairs.FirstOrDefault(kvp => kvp.Key == key) is { } kvpPair
            ? kvpPair.Value
            : null;

        Assert.That(value, Is.Null, $@"Found logging state (""{key}"", {value}");
    }
}

public static class GaussDBConnectionExtensions
{
    public static int ExecuteNonQuery(this GaussDBConnection conn, string sql, GaussDBTransaction? tx = null)
    {
        using var command = tx == null ? new GaussDBCommand(sql, conn) : new GaussDBCommand(sql, conn, tx);
        return command.ExecuteNonQuery();
    }

    public static object? ExecuteScalar(this GaussDBConnection conn, string sql, GaussDBTransaction? tx = null)
    {
        using var command = tx == null ? new GaussDBCommand(sql, conn) : new GaussDBCommand(sql, conn, tx);
        return command.ExecuteScalar();
    }

    public static async Task<int> ExecuteNonQueryAsync(
        this GaussDBConnection conn, string sql, GaussDBTransaction? tx = null, CancellationToken cancellationToken = default)
    {
        await using var command = tx == null ? new GaussDBCommand(sql, conn) : new GaussDBCommand(sql, conn, tx);
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public static async Task<object?> ExecuteScalarAsync(
        this GaussDBConnection conn, string sql, GaussDBTransaction? tx = null, CancellationToken cancellationToken = default)
    {
        await using var command = tx == null ? new GaussDBCommand(sql, conn) : new GaussDBCommand(sql, conn, tx);
        return await command.ExecuteScalarAsync(cancellationToken);
    }
}

public static class GaussDBDataSourceExtensions
{
    public static int ExecuteNonQuery(this GaussDBDataSource dataSource, string sql)
    {
        using var command = dataSource.CreateCommand(sql);
        return command.ExecuteNonQuery();
    }

    public static object? ExecuteScalar(this GaussDBDataSource dataSource, string sql)
    {
        using var command = dataSource.CreateCommand(sql);
        return command.ExecuteScalar();
    }

    public static async Task<int> ExecuteNonQueryAsync(
        this GaussDBDataSource dataSource, string sql, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(sql);
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public static async Task<object?> ExecuteScalarAsync(
        this GaussDBDataSource dataSource, string sql, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(sql);
        return await command.ExecuteScalarAsync(cancellationToken);
    }
}

public static class CommandBehaviorExtensions
{
    public static bool IsSequential(this CommandBehavior behavior)
        => (behavior & CommandBehavior.SequentialAccess) != 0;
}

public static class GaussDBCommandExtensions
{
    public static void WaitUntilCommandIsInProgress(this GaussDBCommand command)
    {
        while (command.State != CommandState.InProgress)
            Thread.Sleep(50);
    }
}

/// <summary>
/// Semantic attribute that points to an issue linked with this test (e.g. this
/// test reproduces the issue)
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
public class IssueLink(string linkAddress) : Attribute
{
    public string LinkAddress { get; private set; } = linkAddress;
}

public enum PrepareOrNot
{
    Prepared,
    NotPrepared
}

public enum PooledOrNot
{
    Pooled,
    Unpooled
}
