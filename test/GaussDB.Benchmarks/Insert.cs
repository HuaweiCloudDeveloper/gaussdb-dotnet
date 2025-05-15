using System.Text;
using BenchmarkDotNet.Attributes;
using GaussDBTypes;

namespace GaussDB.Benchmarks;

public class Insert
{
    GaussDBConnection _conn = default!;
    GaussDBCommand _truncateCmd = default!;

    [Params(1, 100, 1000, 10000)]
    public int BatchSize { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        var connString = new GaussDBConnectionStringBuilder(BenchmarkEnvironment.ConnectionString)
        {
            Pooling = false
        }.ToString();
        _conn = new GaussDBConnection(connString);
        _conn.Open();

        using (var cmd = new GaussDBCommand("CREATE TEMP TABLE data (int1 INT4, text1 TEXT, int2 INT4, text2 TEXT)", _conn))
            cmd.ExecuteNonQuery();

        _truncateCmd = new GaussDBCommand("TRUNCATE data", _conn);
    }

    [GlobalCleanup]
    public void GlobalCleanup() => _conn.Close();

    [Benchmark(Baseline = true)]
    public void Unbatched()
    {
        var cmd = new GaussDBCommand("INSERT INTO data VALUES (@p0, @p1, @p2, @p3)", _conn);
        cmd.Parameters.AddWithValue("p0", GaussDBDbType.Integer, 8);
        cmd.Parameters.AddWithValue("p1", GaussDBDbType.Text, "foo");
        cmd.Parameters.AddWithValue("p2", GaussDBDbType.Integer, 9);
        cmd.Parameters.AddWithValue("p3", GaussDBDbType.Text, "bar");
        cmd.Prepare();

        for (var i = 0; i < BatchSize; i++)
            cmd.ExecuteNonQuery();
        _truncateCmd.ExecuteNonQuery();
    }

    [Benchmark]
    public void Batched()
    {
        var cmd = new GaussDBCommand { Connection = _conn };
        var sb = new StringBuilder();
        for (var i = 0; i < BatchSize; i++)
        {
            var p1 = (i * 4).ToString();
            var p2 = (i * 4 + 1).ToString();
            var p3 = (i * 4 + 2).ToString();
            var p4 = (i * 4 + 3).ToString();
            sb.Append("INSERT INTO data VALUES (@").Append(p1).Append(", @").Append(p2).Append(", @").Append(p3).Append(", @").Append(p4).Append(");");
            cmd.Parameters.AddWithValue(p1, GaussDBDbType.Integer, 8);
            cmd.Parameters.AddWithValue(p2, GaussDBDbType.Text, "foo");
            cmd.Parameters.AddWithValue(p3, GaussDBDbType.Integer, 9);
            cmd.Parameters.AddWithValue(p4, GaussDBDbType.Text, "bar");
        }
        cmd.CommandText = sb.ToString();
        cmd.Prepare();
        cmd.ExecuteNonQuery();
        _truncateCmd.ExecuteNonQuery();
    }

    [Benchmark]
    public void Copy()
    {
        using (var s = _conn.BeginBinaryImport("COPY data (int1, text1, int2, text2) FROM STDIN BINARY"))
        {
            for (var i = 0; i < BatchSize; i++)
            {
                s.StartRow();
                s.Write(8);
                s.Write("foo");
                s.Write(9);
                s.Write("bar");
            }
        }
        _truncateCmd.ExecuteNonQuery();
    }
}