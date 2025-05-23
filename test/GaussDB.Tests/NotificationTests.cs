using NUnit.Framework;
using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using static HuaweiCloud.GaussDB.Tests.TestUtil;

namespace HuaweiCloud.GaussDB.Tests;
//todo: 不支持LISTEN、NOTIFY
/*public class NotificationTests : TestBase
{
    [Test, Description("Simple LISTEN/NOTIFY scenario")]
    public void Notification()
    {
        var notify = GetUniqueIdentifier(nameof(NotificationTests));

        using var conn = OpenConnection();
        var receivedNotification = false;
        conn.ExecuteNonQuery($"LISTEN {notify}");
        conn.Notification += (o, e) => receivedNotification = true;
        conn.ExecuteNonQuery($"NOTIFY {notify}");
        Assert.IsTrue(receivedNotification);
    }

    [Test, Description("Generates a notification that arrives after reader data that is already being read")]
    [IssueLink("https://github.com/npgsql/npgsql/issues/252")]
    public async Task Notification_after_data()
    {
        var notify = GetUniqueIdentifier(nameof(NotificationTests));

        var receivedNotification = false;
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"LISTEN {notify}";
        cmd.ExecuteNonQuery();
        conn.Notification += (o, e) => receivedNotification = true;

        cmd.CommandText = "SELECT generate_series(1,10000)";
        using (var reader = cmd.ExecuteReader())
        {
            //After "notify notifytest1", a notification message will be sent to client,
            //And so the notification message will stick with the last response message of "select generate_series(1,10000)" in GaussDB's tcp receiving buffer.
            using (var conn2 = new GaussDBConnection(ConnectionString))
            {
                conn2.Open();
                using (var command = conn2.CreateCommand())
                {
                    command.CommandText = $"NOTIFY {notify}";
                    command.ExecuteNonQuery();
                }
            }

            // Allow some time for the notification to get delivered
            await Task.Delay(2000);

            Assert.IsTrue(reader.Read());
            Assert.AreEqual(1, reader.GetValue(0));
        }

        Assert.That(conn.ExecuteScalar("SELECT 1"), Is.EqualTo(1));
        Assert.IsTrue(receivedNotification);
    }

    [Test, IssueLink("https://github.com/npgsql/npgsql/issues/1024")]
    public void Wait()
    {
        var notify = GetUniqueIdentifier(nameof(NotificationTests));

        using var conn = OpenConnection();
        using var notifyingConn = OpenConnection();
        var receivedNotification = false;
        conn.ExecuteNonQuery($"LISTEN {notify}");
        notifyingConn.ExecuteNonQuery($"NOTIFY {notify}");
        conn.Notification += (o, e) => receivedNotification = true;
        Assert.That(conn.Wait(0), Is.EqualTo(true));
        Assert.IsTrue(receivedNotification);
        Assert.That(conn.ExecuteScalar("SELECT 1"), Is.EqualTo(1));
    }

    [Test, IssueLink("https://github.com/npgsql/npgsql/issues/1024")]
    public void Wait_with_timeout()
    {
        using var conn = OpenConnection();
        Assert.That(conn.Wait(100), Is.EqualTo(false));
        Assert.That(conn.ExecuteScalar("SELECT 1"), Is.EqualTo(1));
    }

    [Test]
    public void Wait_with_prepended_message()
    {
        using var dataSource = CreateDataSource();
        using (dataSource.OpenConnection()) {}  // A DISCARD ALL is now prepended in the connection's write buffer
        using var conn = dataSource.OpenConnection();
        Assert.That(conn.Wait(100), Is.EqualTo(false));
    }

    [Test, IssueLink("https://github.com/npgsql/npgsql/issues/1024")]
    public async Task WaitAsync()
    {
        var notify = GetUniqueIdentifier(nameof(NotificationTests));

        await using var conn = await OpenConnectionAsync();
        await using var notifyingConn = await OpenConnectionAsync();
        var receivedNotification = false;
        await conn.ExecuteNonQueryAsync($"LISTEN {notify}");
        await notifyingConn.ExecuteNonQueryAsync($"NOTIFY {notify}");
        conn.Notification += (o, e) => receivedNotification = true;
        await conn.WaitAsync(0);
        Assert.IsTrue(receivedNotification);
        Assert.That(await conn.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
    }

    [Test]
    public void WaitAsync_with_timeout()
    {
        using var conn = OpenConnection();
        Assert.That(async () => await conn.WaitAsync(100), Is.EqualTo(false));
        Assert.That(conn.ExecuteScalar("SELECT 1"), Is.EqualTo(1));
    }

    [Test]
    public void Wait_with_keepalive()
    {
        var notify = GetUniqueIdentifier(nameof(NotificationTests));

        using var dataSource = CreateDataSource(csb =>
        {
            csb.KeepAlive = 1;
            csb.Pooling = false;
        });
        using var conn = dataSource.OpenConnection();
        using var notifyingConn = dataSource.OpenConnection();
        conn.ExecuteNonQuery($"LISTEN {notify}");
        var notificationTask = Task.Delay(2000).ContinueWith(t => notifyingConn.ExecuteNonQuery($"NOTIFY {notify}"));
        conn.Wait();
        Assert.That(conn.ExecuteScalar("SELECT 1"), Is.EqualTo(1));
        // A safeguard against closing an active connection
        notificationTask.GetAwaiter().GetResult();
        //Assert.That(TestLoggerSink.Records, Has.Some.With.Property("EventId").EqualTo(new EventId(GaussDBEventId.Keepalive)));
    }

    [Test]
    public async Task WaitAsync_with_keepalive()
    {
        var notify = GetUniqueIdentifier(nameof(NotificationTests));

        await using var dataSource = CreateDataSource(csb =>
        {
            csb.KeepAlive = 1;
            csb.Pooling = false;
        });
        await using var conn = await dataSource.OpenConnectionAsync();
        await using var notifyingConn = await dataSource.OpenConnectionAsync();
        await conn.ExecuteNonQueryAsync($"LISTEN {notify}");
        var notificationTask = Task.Delay(2000).ContinueWith(t => notifyingConn.ExecuteNonQuery($"NOTIFY {notify}"));
        await conn.WaitAsync();
        //Assert.That(TestLoggerSink.Records, Has.Some.With.Property("EventId").EqualTo(new EventId(GaussDBEventId.Keepalive)));
        Assert.That(await conn.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
        // A safeguard against closing an active connection
        await notificationTask;
    }

    [Test]
    public void WaitAsync_cancellation()
    {
        var notify = GetUniqueIdentifier(nameof(NotificationTests));

        using (var conn = OpenConnection())
        {
            Assert.ThrowsAsync<OperationCanceledException>(async () => await conn.WaitAsync(new CancellationToken(true)));
            Assert.That(conn.ExecuteScalar("SELECT 1"), Is.EqualTo(1));
        }

        using (var conn = OpenConnection())
        {
            conn.ExecuteNonQuery($"LISTEN {notify}");
            var cts = new CancellationTokenSource(1000);
            Assert.ThrowsAsync<OperationCanceledException>(async () => await conn.WaitAsync(cts.Token));
            Assert.That(conn.ExecuteScalar("SELECT 1"), Is.EqualTo(1));
        }
    }

    [Test]
    public void Wait_breaks_connection()
    {
        using var dataSource = CreateDataSource();
        using var conn = dataSource.OpenConnection();
        Task.Delay(1000).ContinueWith(t =>
        {
            using var conn2 = OpenConnection();
            conn2.ExecuteNonQuery($"SELECT pg_terminate_backend({conn.ProcessID})");
        });

        var pgEx = Assert.Throws<PostgresException>(conn.Wait)!;
        Assert.That(pgEx.SqlState, Is.EqualTo(PostgresErrorCodes.AdminShutdown));
        Assert.That(conn.FullState, Is.EqualTo(ConnectionState.Broken));
    }

    [Test]
    public void WaitAsync_breaks_connection()
    {
        using var dataSource = CreateDataSource();
        using var conn = dataSource.OpenConnection();
        Task.Delay(1000).ContinueWith(t =>
        {
            using var conn2 = OpenConnection();
            conn2.ExecuteNonQuery($"SELECT pg_terminate_backend({conn.ProcessID})");
        });

        var pgEx = Assert.ThrowsAsync<PostgresException>(async () => await conn.WaitAsync())!;
        Assert.That(pgEx.SqlState, Is.EqualTo(PostgresErrorCodes.AdminShutdown));
        Assert.That(conn.FullState, Is.EqualTo(ConnectionState.Broken));
    }

    [Test, IssueLink("https://github.com/npgsql/npgsql/issues/4911")]
    public async Task Big_notice_while_loading_types()
    {
        await using var adminConn = await OpenConnectionAsync();
        // Max notification payload is 8000
        await using var dataSource = CreateDataSource(csb => csb.ReadBufferSize = 4096);
        await using var conn = await dataSource.OpenConnectionAsync();

        var notify = GetUniqueIdentifier(nameof(Big_notice_while_loading_types));
        await conn.ExecuteNonQueryAsync($"LISTEN {notify}");
        var payload = new string('a', 5000);
        await adminConn.ExecuteNonQueryAsync($"NOTIFY {notify}, '{payload}'");

        await conn.ReloadTypesAsync();
    }
}*/
