GaussDB is the open source .NET data provider for PostgreSQL. It allows you to connect and interact with PostgreSQL server using .NET.

This package is an GaussDB plugin which allows you to interact with spatial data provided by the PostgreSQL [PostGIS extension](https://postgis.net); PostGIS is a mature, standard extension considered to provide top-of-the-line database spatial features. On the .NET side, the plugin adds support for the types from the [NetTopologySuite library](https://github.com/NetTopologySuite/NetTopologySuite), allowing you to read and write them directly to PostgreSQL. 

To use the NetTopologySuite plugin, add a dependency on this package and create a GaussDBDataSource.

```csharp
using HuaweiCloud.GaussDB;
using NetTopologySuite.Geometries;

var dataSourceBuilder = new GaussDBDataSourceBuilder(ConnectionString);

dataSourceBuilder.UseNetTopologySuite();

var dataSource = dataSourceBuilder.Build();
var conn = await dataSource.OpenConnectionAsync();

var point = new Point(new Coordinate(1d, 1d));
conn.ExecuteNonQuery("CREATE TEMP TABLE data (geom GEOMETRY)");
using (var cmd = new GaussDBCommand("INSERT INTO data (geom) VALUES (@p)", conn))
{
    cmd.Parameters.AddWithValue("@p", point);
    cmd.ExecuteNonQuery();
}

using (var cmd = new GaussDBCommand("SELECT geom FROM data", conn))
using (var reader = cmd.ExecuteReader())
{
    reader.Read();
    Assert.That(reader[0], Is.EqualTo(point));
}
```

For more information, [visit the NetTopologySuite plugin documentation page](https://www.gaussdb.org/doc/types/nts.html).
