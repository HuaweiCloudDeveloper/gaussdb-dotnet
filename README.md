# GaussDB Npgsql Driver

[![PostgreSQL License](https://img.shields.io/badge/License-PostgreSQL-blue.svg)](https://opensource.org/licenses/PostgreSQL)



## What is GaussDB ?

GuassDB is an open-source database driver led by Huawei's open-source community and refactored based on npgsql. It is compatible with openGauss and Guass databases. [Open Source for Huawei](https://developer.huaweicloud.com/programs/opensource/contributing/) revolves around Huawei's technology ecosystems including Kunpeng, Ascend, HarmonyOS, and Huawei Cloud. Through collaboration with developers from enterprises, universities, and the open-source community, it enables adaptation development and solution validation for open-source software, ensuring smoother and more efficient operation on Huawei Cloud.  

Before getting started, developers can download the [Open Source for Huawei Wiki](https://gitcode.com/HuaweiCloudDeveloper/OpenSourceForHuaweiWiki) to access detailed development procedures, technical preparations, and various resources required throughout the development process. If you have any questions during use, please visit the [GaussDB forum](https://bbs.huaweicloud.com/forum/forum-1350-1.html) for discussion.



## Quick start

Here's a basic code snippet to get you started:

```csharp
using GaussDB;

var connString = "Host=myserver;Username=mylogin;Password=mypass;Database=mydatabase";

var dataSourceBuilder = new GaussDBDataSourceBuilder(connString);
var dataSource = dataSourceBuilder.Build();

var conn = await dataSource.OpenConnectionAsync();

await using (var cmd = new GaussDBCommand("INSERT INTO data (some_field) VALUES (@p)", conn))
{
    cmd.Parameters.AddWithValue("p", "Hello world");
    await cmd.ExecuteNonQueryAsync();
}

await using (var cmd = new GaussCommand("SELECT some_field FROM data", conn))
await using (var reader = await cmd.ExecuteReaderAsync())
{
    while (await reader.ReadAsync())
        Console.WriteLine(reader.GetString(0));
}
```





## License
This project is released under of the [PostgreSQL License](https://opensource.org/license/PostgreSQL).
