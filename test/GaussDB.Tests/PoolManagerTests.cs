using NUnit.Framework;

namespace GaussDB.Tests;

[NonParallelizable]
class PoolManagerTests : TestBase
{
    [Test]
    public void With_canonical_connection_string()
    {
        var connString = new GaussDBConnectionStringBuilder(ConnectionString).ToString();
        using (var conn = new GaussDBConnection(connString))
            conn.Open();
        var connString2 = new GaussDBConnectionStringBuilder(ConnectionString)
        {
            ApplicationName = "Another connstring"
        }.ToString();
        using (var conn = new GaussDBConnection(connString2))
            conn.Open();
    }

#if DEBUG
    [Test]
    public void Many_pools()
    {
        PoolManager.Reset();
        for (var i = 0; i < 15; i++)
        {
            var connString = new GaussDBConnectionStringBuilder(ConnectionString)
            {
                ApplicationName = "App" + i
            }.ToString();
            using var conn = new GaussDBConnection(connString);
            conn.Open();
        }
        PoolManager.Reset();
    }
#endif

    [Test]
    public void ClearAllPools()
    {
        using (var conn = new GaussDBConnection(ConnectionString))
            conn.Open();
        // Now have one connection in the pool
        Assert.That(PoolManager.Pools.TryGetValue(ConnectionString, out var pool), Is.True);
        Assert.That(pool!.Statistics.Idle, Is.EqualTo(1));

        GaussDBConnection.ClearAllPools();
        Assert.That(pool.Statistics.Idle, Is.Zero);
        Assert.That(pool.Statistics.Total, Is.Zero);
    }

    [Test]
    public void ClearAllPools_with_busy()
    {
        GaussDBDataSource? pool;
        using (var conn = new GaussDBConnection(ConnectionString))
        {
            conn.Open();
            using (var anotherConn = new GaussDBConnection(ConnectionString))
                anotherConn.Open();
            // We have one idle, one busy

            GaussDBConnection.ClearAllPools();
            Assert.That(PoolManager.Pools.TryGetValue(ConnectionString, out pool), Is.True);
            Assert.That(pool!.Statistics.Idle, Is.Zero);
            Assert.That(pool.Statistics.Total, Is.EqualTo(1));
        }
        Assert.That(pool.Statistics.Idle, Is.Zero);
        Assert.That(pool.Statistics.Total, Is.Zero);
    }

    [SetUp]
    public void Setup() => PoolManager.Reset();

    [TearDown]
    public void Teardown() => PoolManager.Reset();
}
