using Npgsql;

namespace GaussDemo;

class Program
{
    static async Task Main(string[] args)
    {
        var connString = @"";
        var dataSourceBuilder = new NpgsqlDataSourceBuilder(connString);
        var dataSource = dataSourceBuilder.Build();
        var conn = await dataSource.OpenConnectionAsync();
    }
}
