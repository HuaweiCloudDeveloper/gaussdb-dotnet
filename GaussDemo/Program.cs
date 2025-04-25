using Npgsql;

namespace GaussDemo;

class Program
{
    static async Task Main(string[] args)
    {
        var connString = @"Host=120.46.168.209:8000;Username=root;Password=huawei@db~123;Database=gauss-9ffe;PasswordAuthentication=SHA256";
        var dataSourceBuilder = new NpgsqlDataSourceBuilder(connString);
        var dataSource = dataSourceBuilder.Build();
        var conn = await dataSource.OpenConnectionAsync();


    }
}
