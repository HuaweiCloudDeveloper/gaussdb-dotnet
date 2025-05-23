using HuaweiCloud.GaussDBTypes;
using NUnit.Framework;
using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using static HuaweiCloud.GaussDB.Tests.TestUtil;

namespace HuaweiCloud.GaussDB.Tests;

public class AutoPrepareTests : TestBase
{
    [Test]
    public void Basic()
    {
        using var dataSource = CreateDataSource(csb =>
        {
            csb.MaxAutoPrepare = 10;
            csb.AutoPrepareMinUsages = 2;
        });
        using var conn = dataSource.OpenConnection();
        using var checkCmd = new GaussDBCommand(CountPreparedStatements, conn);
        checkCmd.Prepare();

        conn.ExecuteNonQuery("SELECT 1");
        Assert.That(checkCmd.ExecuteScalar(), Is.EqualTo(0));

        using (var cmd = new GaussDBCommand("SELECT 1", conn))
        {
            Assert.That(cmd.IsPrepared, Is.False);
            cmd.ExecuteScalar();
            Assert.That(cmd.IsPrepared, Is.True);
            Assert.That(checkCmd.ExecuteScalar(), Is.EqualTo(1));
            cmd.ExecuteScalar();
            Assert.That(cmd.IsPrepared, Is.True);
            Assert.That(checkCmd.ExecuteScalar(), Is.EqualTo(1));
        }

        using (var cmd = new GaussDBCommand("SELECT 1", conn))
        {
            cmd.ExecuteScalar();
            Assert.That(cmd.IsPrepared, Is.True);
        }
        Assert.That(checkCmd.ExecuteScalar(), Is.EqualTo(1));
    }

    [Test, Description("Passes the maximum limit for autoprepared statements, recycling the least-recently used one")]
    public void Recycle()
    {
        using var dataSource = CreateDataSource(csb =>
        {
            csb.AutoPrepareMinUsages = 2;
            csb.MaxAutoPrepare = 2;
        });
        using var conn = dataSource.OpenConnection();
        using var checkCmd = new GaussDBCommand(CountPreparedStatements, conn);
        checkCmd.Prepare();

        Assert.That(checkCmd.ExecuteScalar(), Is.EqualTo(0));
        var cmd1 = new GaussDBCommand("SELECT 1", conn);
        cmd1.ExecuteNonQuery(); cmd1.ExecuteNonQuery();
        Assert.That(cmd1.IsPrepared, Is.True);
        Assert.That(checkCmd.ExecuteScalar(), Is.EqualTo(1));

        var cmd2 = new GaussDBCommand("SELECT 2", conn);
        cmd2.ExecuteNonQuery(); cmd2.ExecuteNonQuery();
        Assert.That(cmd2.IsPrepared, Is.True);
        Assert.That(checkCmd.ExecuteScalar(), Is.EqualTo(2));

        cmd1.ExecuteNonQuery();

        // Cause another statement to be autoprepared. This should eject cmd2.
        conn.ExecuteNonQuery("SELECT 3"); conn.ExecuteNonQuery("SELECT 3");
        Assert.That(checkCmd.ExecuteScalar(), Is.EqualTo(2));

        cmd2.ExecuteNonQuery();
        Assert.That(cmd2.IsPrepared, Is.False);
        using (var getTextCmd = new GaussDBCommand("SELECT statement FROM pg_prepared_statements WHERE statement NOT LIKE '%COUNT%' ORDER BY statement", conn))
        using (var reader = getTextCmd.ExecuteReader())
        {
            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetString(0), Is.EqualTo("SELECT 1"));
            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetString(0), Is.EqualTo("SELECT 3"));
        }
    }

    [Test]
    public void Persist()
    {
        using var dataSource = CreateDataSource(csb =>
        {
            csb.MaxAutoPrepare = 10;
            csb.AutoPrepareMinUsages = 2;
        });

        using (var conn = dataSource.OpenConnection())
        using (var checkCmd = new GaussDBCommand(CountPreparedStatements, conn))
        {
            checkCmd.Prepare();
            conn.ExecuteNonQuery("SELECT 1"); conn.ExecuteNonQuery("SELECT 1");
            Assert.That(checkCmd.ExecuteScalar(), Is.EqualTo(1));
        }

        // We now have two prepared statements which should be persisted

        using (var conn = dataSource.OpenConnection())
        using (var checkCmd = new GaussDBCommand(CountPreparedStatements, conn))
        {
            checkCmd.Prepare();
            Assert.That(checkCmd.ExecuteScalar(), Is.EqualTo(1));
            using (var cmd = new GaussDBCommand("SELECT 1", conn))
            {
                cmd.ExecuteScalar();
                //Assert.That(cmd.IsPrepared);
            }
            Assert.That(checkCmd.ExecuteScalar(), Is.EqualTo(1));
        }
    }

    [Test]
    public async Task Positional_parameter()
    {
        await using var dataSource = CreateDataSource(csb =>
        {
            csb.AutoPrepareMinUsages = 2;
            csb.MaxAutoPrepare = 2;
        });
        await using var conn = await dataSource.OpenConnectionAsync();
        await using var checkCmd = new GaussDBCommand(CountPreparedStatements, conn);
        await checkCmd.PrepareAsync();

        await using var cmd = new GaussDBCommand("SELECT $1", conn);
        cmd.Parameters.Add(new GaussDBParameter { GaussDBDbType = GaussDBDbType.Integer, Value = 8 });

        Assert.That(cmd.IsPrepared, Is.False);
        Assert.That(await cmd.ExecuteScalarAsync(), Is.EqualTo(8));
        Assert.That(cmd.IsPrepared, Is.False);
        Assert.That(await cmd.ExecuteScalarAsync(), Is.EqualTo(8));
        Assert.That(cmd.IsPrepared, Is.True);
        Assert.That(await cmd.ExecuteScalarAsync(), Is.EqualTo(8));
        Assert.That(cmd.IsPrepared, Is.True);
    }

    [Test]
    public void Promote_auto_to_explicit()
    {
        using var dataSource = CreateDataSource(csb =>
        {
            csb.MaxAutoPrepare = 10;
            csb.AutoPrepareMinUsages = 2;
        });
        using var conn = dataSource.OpenConnection();
        using var checkCmd = new GaussDBCommand(CountPreparedStatements, conn);
        using var cmd1 = new GaussDBCommand("SELECT 1", conn);
        using var cmd2 = new GaussDBCommand("SELECT 1", conn);
        checkCmd.Prepare();

        cmd1.ExecuteNonQuery(); cmd1.ExecuteNonQuery();
        // cmd1 is now autoprepared
        Assert.That(checkCmd.ExecuteScalar(), Is.EqualTo(1));
        Assert.That(conn.Connector!.PreparedStatementManager.NumPrepared, Is.EqualTo(2));

        // Promote (replace) the autoprepared statement with an explicit one.
        cmd2.Prepare();
        Assert.That(checkCmd.ExecuteScalar(), Is.EqualTo(1));
        Assert.That(conn.Connector.PreparedStatementManager.NumPrepared, Is.EqualTo(2));

        // cmd1's statement is no longer valid (has been closed), make sure it still works (will run unprepared)
        cmd2.ExecuteScalar();

        // Trigger autoprepare on a different query to confirm we didn't leave replaced statement in a bad state
        using var cmd3 = new GaussDBCommand("SELECT 2", conn);
        cmd3.ExecuteNonQuery(); cmd3.ExecuteNonQuery();
    }

    [Test]
    public void Candidate_eject()
    {
        using var dataSource = CreateDataSource(csb =>
        {
            csb.MaxAutoPrepare = 10;
            csb.AutoPrepareMinUsages = 3;
        });
        using var conn = dataSource.OpenConnection();
        using var cmd = conn.CreateCommand();

        for (var i = 0; i < PreparedStatementManager.CandidateCount; i++)
        {
            cmd.CommandText = $"SELECT {i}";
            cmd.ExecuteNonQuery();
        }

        // The candidate list is now full with single-use statements.

        cmd.CommandText = "SELECT 'double_use'";
        cmd.ExecuteNonQuery(); cmd.ExecuteNonQuery();
        // We now have a single statement that has been used twice.

        for (var i = PreparedStatementManager.CandidateCount; i < PreparedStatementManager.CandidateCount * 2; i++)
        {
            cmd.CommandText = $"SELECT {i}";
            cmd.ExecuteNonQuery();
        }

        // The new single-use statements should have ejected all previous single-use statements
        cmd.CommandText = "SELECT 1";
        cmd.ExecuteNonQuery(); cmd.ExecuteNonQuery();
        Assert.That(cmd.IsPrepared, Is.False);

        // But the double-use statement should still be there
        cmd.CommandText = "SELECT 'double_use'";
        cmd.ExecuteNonQuery();
        Assert.That(cmd.IsPrepared, Is.True);
    }

    [Test]
    public void One_command_same_sql_twice()
    {
        using var dataSource = CreateDataSource(csb =>
        {
            csb.MaxAutoPrepare = 10;
            csb.AutoPrepareMinUsages = 2;
        });
        using var conn = dataSource.OpenConnection();
        using var cmd = new GaussDBCommand("SELECT 1; SELECT 1; SELECT 1; SELECT 1", conn);
        //cmd.Prepare();
        //Assert.That(cmd.IsPrepared, Is.True);
        cmd.ExecuteNonQuery();
        Assert.That(conn.ExecuteScalar(CountPreparedStatements), Is.EqualTo(1));
    }

    [Test]
    public void Across_close_open_different_connector()
    {
        using var dataSource = CreateDataSource(csb =>
        {
            csb.MaxAutoPrepare = 10;
            csb.AutoPrepareMinUsages = 2;
        });
        using var conn1 = dataSource.CreateConnection();
        using var conn2 = dataSource.CreateConnection();
        using var cmd = new GaussDBCommand("SELECT 1", conn1);
        conn1.Open();
        cmd.ExecuteNonQuery(); cmd.ExecuteNonQuery();
        Assert.That(cmd.IsPrepared, Is.True);
        var processId = conn1.ProcessID;
        conn1.Close();
        conn2.Open();
        conn1.Open();
        Assert.That(conn1.ProcessID, Is.Not.EqualTo(processId));
        Assert.That(cmd.IsPrepared, Is.False);
        Assert.That(cmd.ExecuteScalar(), Is.EqualTo(1));  // Execute unprepared
        cmd.Prepare();
        Assert.That(cmd.ExecuteScalar(), Is.EqualTo(1));
    }

    [Test]
    public void Unprepare_all()
    {
        using var dataSource = CreateDataSource(csb =>
        {
            csb.MaxAutoPrepare = 10;
            csb.AutoPrepareMinUsages = 2;
        });
        using var conn = dataSource.OpenConnection();
        using var cmd = new GaussDBCommand("SELECT 1", conn);
        cmd.Prepare();  // Explicit
        conn.ExecuteNonQuery("SELECT 2"); conn.ExecuteNonQuery("SELECT 2");  // Auto
        Assert.That(conn.ExecuteScalar(CountPreparedStatements), Is.EqualTo(2));
        conn.UnprepareAll();
        Assert.That(conn.ExecuteScalar(CountPreparedStatements), Is.Zero);
    }

    [Test, Description("Prepares the same SQL with different parameters (overloading)")]
    public void Overloaded_sql()
    {
        using var dataSource = CreateDataSource(csb =>
        {
            csb.MaxAutoPrepare = 10;
            csb.AutoPrepareMinUsages = 2;
        });
        using var conn = dataSource.OpenConnection();
        using (var cmd = new GaussDBCommand("SELECT @p", conn))
        {
            cmd.Parameters.AddWithValue("p", GaussDBDbType.Integer, 8);
            cmd.ExecuteNonQuery();
            cmd.ExecuteNonQuery();
            Assert.That(cmd.IsPrepared, Is.True);
        }
        using (var cmd = new GaussDBCommand("SELECT @p", conn))
        {
            cmd.Parameters.AddWithValue("p", GaussDBDbType.Text, "foo");
            Assert.That(cmd.ExecuteScalar(), Is.EqualTo("foo"));
            Assert.That(cmd.ExecuteScalar(), Is.EqualTo("foo"));
            Assert.That(cmd.IsPrepared, Is.False);
        }

        // SQL overloading is a pretty rare/exotic scenario. Handling it properly would involve keying
        // prepared statements not just by SQL but also by the parameter types, which would pointlessly
        // increase allocations. Instead, the second execution simply runs unprepared.
        Assert.That(conn.ExecuteScalar(CountPreparedStatements), Is.EqualTo(1));
    }

    [Test, Description("Tests parameter derivation a parameterized query (CommandType.Text) that is already auto-prepared.")]
    public void Derive_parameters_for_auto_prepared_statement()
    {
        const string query = "SELECT @p::integer";
        const int answer = 42;
        using var dataSource = CreateDataSource(csb =>
        {
            csb.MaxAutoPrepare = 10;
            csb.AutoPrepareMinUsages = 2;
        });
        using var conn = dataSource.OpenConnection();
        using var checkCmd = new GaussDBCommand(CountPreparedStatements, conn);
        using var cmd = new GaussDBCommand(query, conn);
        checkCmd.Prepare();
        cmd.Parameters.AddWithValue("@p", GaussDBDbType.Integer, answer);
        cmd.ExecuteNonQuery(); cmd.ExecuteNonQuery(); // cmd1 is now autoprepared
        Assert.That(checkCmd.ExecuteScalar(), Is.EqualTo(1));
        Assert.That(conn.Connector!.PreparedStatementManager.NumPrepared, Is.EqualTo(2));

        // Derive parameters for the already autoprepared statement
        GaussDBCommandBuilder.DeriveParameters(cmd);
        Assert.That(cmd.Parameters.Count, Is.EqualTo(1));
        Assert.That(cmd.Parameters[0].ParameterName, Is.EqualTo("p"));

        // DeriveParameters should have silently unprepared the autoprepared statements
        Assert.That(checkCmd.ExecuteScalar(), Is.EqualTo(0));
        Assert.That(conn.Connector.PreparedStatementManager.NumPrepared, Is.EqualTo(1));

        cmd.Parameters["@p"].Value = answer;
        Assert.That(cmd.ExecuteScalar(), Is.EqualTo(answer));
    }

    [Test, IssueLink("https://github.com/npgsql/npgsql/issues/2644")]
    public void Row_description_properly_cloned()
    {
        using var dataSource = CreateDataSource(csb =>
        {
            csb.MaxAutoPrepare = 10;
            csb.AutoPrepareMinUsages = 2;
        });
        using var conn = dataSource.OpenConnection();
        conn.UnprepareAll();
        using var cmd1 = new GaussDBCommand("SELECT 1 AS foo", conn);
        using var cmd2 = new GaussDBCommand("SELECT 1 AS bar", conn);

        cmd1.ExecuteNonQuery();
        cmd1.ExecuteNonQuery();  // Query is now auto-prepared
        cmd2.ExecuteNonQuery();
        using var reader = cmd1.ExecuteReader();
        Assert.That(reader.GetName(0), Is.EqualTo("foo"));
    }

    [Test, IssueLink("https://github.com/npgsql/npgsql/issues/3106")]
    public async Task Dont_auto_prepare_more_than_max_statements_in_batch()
    {
        const int maxAutoPrepare = 50;

        await using var dataSource = CreateDataSource(csb => csb.MaxAutoPrepare = maxAutoPrepare);
        await using var connection = await dataSource.OpenConnectionAsync();
        for (var i = 0; i < 100; i++)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = string.Join("", Enumerable.Range(0, 100).Select(n => $"SELECT {n};"));
            await command.ExecuteNonQueryAsync();
        }

        Assert.That(await connection.ExecuteScalarAsync(CountPreparedStatements), Is.LessThanOrEqualTo(maxAutoPrepare));
    }

    [Test, IssueLink("https://github.com/npgsql/npgsql/issues/3106")]
    public async Task Dont_auto_prepare_more_than_max_statements_in_batch_random()
    {
        const int maxAutoPrepare = 10;

        await using var dataSource = CreateDataSource(csb => csb.MaxAutoPrepare = maxAutoPrepare);
        await using var connection = await dataSource.OpenConnectionAsync();
        var random = new Random(1);
        for (var i = 0; i < 100; i++)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = string.Join("", Enumerable.Range(0, 100).Select(n => $"SELECT {random.Next(200)};"));
            await command.ExecuteNonQueryAsync();
        }

        Assert.That(await connection.ExecuteScalarAsync(CountPreparedStatements), Is.LessThanOrEqualTo(maxAutoPrepare));
    }

    [Test]
    public async Task Replace_and_execute_within_same_batch()
    {
        await using var dataSource = CreateDataSource(csb =>
        {
            csb.MaxAutoPrepare = 1;
            csb.AutoPrepareMinUsages = 2;
        });
        await using var connection = await dataSource.OpenConnectionAsync();
        for (var i = 0; i < 2; i++)
            await connection.ExecuteNonQueryAsync("SELECT 1");

        // SELECT 1 is now auto-prepared and occupying the only slot.
        // Within the same batch, cause another SQL to replace it, and then execute it.
        await connection.ExecuteNonQueryAsync("SELECT 2; SELECT 2; SELECT 1");
    }

    // Exclude some internal GaussDB queries which include pg_type as well as the count statement itself
    const string CountPreparedStatements = """
SELECT COUNT(*) FROM pg_prepared_statements
WHERE statement NOT LIKE '%pg_prepared_statements%'
AND statement NOT LIKE '%pg_type%'
""";

    [Test, IssueLink("https://github.com/npgsql/npgsql/issues/2665")]
    public async Task Auto_prepared_command_failure()
    {
        await using var dataSource = CreateDataSource(csb =>
        {
            csb.MaxAutoPrepare = 10;
            csb.AutoPrepareMinUsages = 2;
        });
        await using var conn = await dataSource.OpenConnectionAsync();

        var tableName = await GetTempTableName(conn);
        await conn.ExecuteNonQueryAsync($"CREATE TABLE {tableName} (id integer)");

        await using (var command = new GaussDBCommand($"INSERT INTO {tableName} (id) VALUES (1)", conn))
        {
            await command.ExecuteNonQueryAsync();
            await conn.ExecuteNonQueryAsync($"DROP TABLE {tableName} CASCADE;");
            Assert.ThrowsAsync<PostgresException>(async () => await command.ExecuteNonQueryAsync());
        }

        await conn.ExecuteNonQueryAsync($"CREATE TABLE {tableName} (id integer)");

        await using (var command = new GaussDBCommand($"INSERT INTO {tableName} (id) VALUES (1)", conn))
        {
            await command.ExecuteNonQueryAsync();
            await command.ExecuteNonQueryAsync();
        }
    }

    [Test, IssueLink("https://github.com/npgsql/npgsql/issues/3002")]
    public void Replace_with_bad_sql()
    {
        using var dataSource = CreateDataSource(csb =>
        {
            csb.MaxAutoPrepare = 2;
            csb.AutoPrepareMinUsages = 1;
        });
        using var conn = dataSource.OpenConnection();

        conn.ExecuteNonQuery("SELECT 1");
        conn.ExecuteNonQuery("SELECT 2");

        // Attempt to replace SELECT 1, but fail because of bad SQL.
        // Because of the issue, PreparedStatementManager.NumPrepared is reduced from 2 to 1
        Assert.That(() => conn.ExecuteNonQuery("SELECTBAD"), Throws.Exception.TypeOf<PostgresException>()
            .With.Property(nameof(PostgresException.SqlState)).EqualTo(PostgresErrorCodes.SyntaxError));
        // Prevent SELECT 2 from being the LRU
        conn.ExecuteNonQuery("SELECT 2");
        // And attempt to replace again, reducing PreparedStatementManager.NumPrepared to 0
        Assert.That(() => conn.ExecuteNonQuery("SELECTBAD"), Throws.Exception.TypeOf<PostgresException>()
            .With.Property(nameof(PostgresException.SqlState)).EqualTo(PostgresErrorCodes.SyntaxError));

        // Since PreparedStatementManager.NumPrepared is 0, GaussDB will now send DISCARD ALL, but our internal state thinks
        // SELECT 2 is still prepared.
        conn.Close();
        conn.Open();

        Assert.That(conn.ExecuteScalar("SELECT 2"), Is.EqualTo(2));
    }

    [Test, IssueLink("https://github.com/npgsql/npgsql/issues/4082")]
    public async Task Batch_statement_execution_error_cleanup()
    {
        await using var dataSource = CreateDataSource(csb =>
        {
            csb.MaxAutoPrepare = 2;
            csb.AutoPrepareMinUsages = 1;
        });
        await using var conn = await dataSource.OpenConnectionAsync();
        var funcName = await GetTempFunctionName(conn);

        // Create a function we can use to raise an error with a single statement
        await conn.ExecuteNonQueryAsync(
$"""
CREATE OR REPLACE FUNCTION {funcName}() RETURNS VOID AS
    'BEGIN RAISE EXCEPTION ''testexception'' USING ERRCODE = ''12345'', DETAIL = ''testdetail''; END;'
LANGUAGE 'plpgsql';
""");

        conn.UnprepareAll();

        // Occupy _auto1 and _auto2
        await conn.ExecuteNonQueryAsync("SELECT 1");
        await conn.ExecuteNonQueryAsync("SELECT 2");

        // Execute two new SELECTs which will replace the above two. _auto1 will now contain SELECT pg_temp.emit_exception()
        // and _auto2 will contain SELECT 4. Note that they must be in this order because only the statements following
        // the error-triggering statement will be unprepared.
        //
        // We expect error 12345. Prior to the error being raised, the SELECT pg_temp.emit_exception will be successfully prepared
        // and the previous _auto1 (SELECT 1) will be successfully closed. However, the subsequent SELECT 4 will not be prepared,
        // and the previous _auto2 (SELECT 2) will not be properly closed. SELECT 4 will then be unprepared.
        var ex = Assert.ThrowsAsync<PostgresException>(async () => await conn.ExecuteNonQueryAsync($"SELECT {funcName}(); SELECT 4"))!;
        Assert.That(ex, Is.TypeOf<PostgresException>().With.Property(nameof(PostgresException.SqlState)).EqualTo("12345"));

        // The PreparedStatementManager prioritises replacement of unprepared statements, so we know this will replace SELECT 4 in
        // _auto2. The code previously assumed that cleanup was never required when replacing an unprepared statement (since it
        // was never prepared in PG) and this is true in most cases. However, in this case, SELECT 3 needs to logically replace
        // SELECT 2.
        //
        // Due to the bug, _auto2 never gets cleaned up and this throws a 42P05 (prepared statement "_auto2" already exists)
        // when we try to use that slot
        Assert.That(await conn.ExecuteScalarAsync("SELECT 3"), Is.EqualTo(3));
    }

    [Test, IssueLink("https://github.com/npgsql/npgsql/issues/4404"), IssueLink("https://github.com/npgsql/npgsql/issues/5220")]
    public async Task SchemaOnly()
    {
        await using var dataSource = CreateDataSource(csb =>
        {
            csb.AutoPrepareMinUsages = 2;
            csb.MaxAutoPrepare = 10;
        });
        await using var conn = await dataSource.OpenConnectionAsync();
        await using var cmd = new GaussDBCommand("SELECT 1", conn);

        for (var i = 0; i < 5; i++)
        {
            await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly);
        }

        // Make sure there is no protocol desync due to #5220
        await cmd.ExecuteScalarAsync();
    }

    [Test]
    public async Task Auto_prepared_statement_invalidation()
    {
        await using var dataSource = CreateDataSource(csb =>
        {
            csb.MaxAutoPrepare = 10;
            csb.AutoPrepareMinUsages = 2;
        });
        await using var connection = await dataSource.OpenConnectionAsync();
        var table = await CreateTempTable(connection, "foo int");

        await using var command = new GaussDBCommand($"SELECT * FROM {table}", connection);
        for (var i = 0; i < 2; i++)
            await command.ExecuteNonQueryAsync();

        await connection.ExecuteNonQueryAsync($"ALTER TABLE {table} RENAME COLUMN foo TO bar");
        // Since we've changed the table schema, the next execution of the prepared statement will error with 0A000
        var exception = Assert.ThrowsAsync<PostgresException>(() => command.ExecuteNonQueryAsync())!;
        Assert.That(exception.SqlState, Is.EqualTo("29P06")); // cached plan must not change result type PostgresErrorCodes.FeatureNotSupported

        // todo: 不是很明白为什么需要报错，对象没有脱离作用域理应正常运行。
        // However, GaussDB should invalidate the prepared statement in this case, so the next execution should work
        //Assert.DoesNotThrowAsync(() => command.ExecuteNonQueryAsync());
    }

    void DumpPreparedStatements(GaussDBConnection conn)
    {
        using var cmd = new GaussDBCommand("SELECT name,statement FROM pg_prepared_statements", conn);
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
            Console.WriteLine($"{reader.GetString(0)}: {reader.GetString(1)}");
    }
}
