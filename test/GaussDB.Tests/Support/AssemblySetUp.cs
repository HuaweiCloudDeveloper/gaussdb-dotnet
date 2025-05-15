using GaussDB;
using GaussDB.Tests;
using NUnit.Framework;
using System;
using System.Threading;

[SetUpFixture]
public class AssemblySetUp
{
    [OneTimeSetUp]
    public void Setup()
    {
        var connString = TestUtil.ConnectionString;
        using var conn = new GaussDBConnection(connString);
        try
        {
            conn.Open();
        }
        catch (PostgresException e)
        {
            if (e.SqlState == PostgresErrorCodes.InvalidPassword && connString == TestUtil.DefaultConnectionString)
                throw new Exception("Please create a user gaussdb_tests as follows: CREATE USER gaussdb_tests PASSWORD 'gaussdb_tests' SUPERUSER");

            if (e.SqlState == PostgresErrorCodes.InvalidCatalogName)
            {
                var builder = new GaussDBConnectionStringBuilder(connString)
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

                conn.Open();
                return;
            }

            throw;
        }
    }
}
