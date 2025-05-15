GaussDB is the open source .NET data provider for PostgreSQL. It allows you to connect and interact with PostgreSQL server using .NET.

This package is an GaussDB plugin which allows you to use the [NodaTime](https://nodatime.org) date/time library when interacting with PostgreSQL; this provides a better and safer API for dealing with date and time data. 

To use the NodaTime plugin, add a dependency on this package and create a GaussDBDataSource. Once this is done, you can use NodaTime types when interacting with PostgreSQL, just as you would use e.g. `DateTime`:

```csharp
using HuaweiCloud.GaussDB;

var dataSourceBuilder = new GaussDBDataSourceBuilder(ConnectionString);

dataSourceBuilder.UseNodaTime();

var dataSource = dataSourceBuilder.Build();
var conn = await dataSource.OpenConnectionAsync();

// Write NodaTime Instant to PostgreSQL "timestamp with time zone" (UTC)
using (var cmd = new GaussDBCommand(@"INSERT INTO mytable (my_timestamptz) VALUES (@p)", conn))
{
    cmd.Parameters.Add(new GaussDBParameter("p", Instant.FromUtc(2011, 1, 1, 10, 30)));
    cmd.ExecuteNonQuery();
}

// Read timestamp back from the database as an Instant
using (var cmd = new GaussDBCommand(@"SELECT my_timestamptz FROM mytable", conn))
using (var reader = cmd.ExecuteReader())
{
    reader.Read();
    var instant = reader.GetFieldValue<Instant>(0);
}
```

For more information, [visit the NodaTime plugin documentation page](https://www.gaussdb.org/doc/types/nodatime.html).
