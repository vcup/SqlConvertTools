using System.CommandLine;
using Microsoft.Data.Sqlite;
using Newtonsoft.Json;
using SqlSugar;

namespace SqlConvertTools.Commands;

public class SqlServerToSqliteCommand : Command
{
    public SqlServerToSqliteCommand()
        : base("mssql2sqlite", string.Empty)
    {
        var mssqlConnectStringArgument = new Argument<string>
        {
            Name = "mssql_conn_str",
            Description = "connect string of microsoft sql server"
        };

        var sqliteDbFileArgument = new Argument<FileInfo>(() => new FileInfo("{random number}.db"))
        {
            Name = "sqlite_path",
            Description = "database file path of sqlite",
        };

        AddArgument(mssqlConnectStringArgument);
        AddArgument(sqliteDbFileArgument);

        this.SetHandler(Run, mssqlConnectStringArgument, sqliteDbFileArgument);
    }

    private static async Task Run(string mssqlConnString, FileSystemInfo? sqliteDb)
    {
        var sqliteConnectStringBuilder = new SqliteConnectionStringBuilder
        {
            DataSource = sqliteDb?.FullName ?? $"{Random.Shared.NextInt64()}.db"
        };
        var sqlite = new SqlSugarScope(new ConnectionConfig
        {
            ConnectionString = sqliteConnectStringBuilder.ToString(),
            IsAutoCloseConnection = true,
            DbType = DbType.Sqlite,
            ConfigId = "sqlite"
        });
        var mssql = new SqlSugarClient(new ConnectionConfig
        {
            ConnectionString = mssqlConnString,
            DbType = DbType.SqlServer,
            IsAutoCloseConnection = true,
            ConfigId = "mssql"
        });

        var tables = mssql.DbMaintenance
            .GetTableInfoList(false);
        foreach (var table in tables)
        {
            var columnInfos = mssql.DbMaintenance.GetColumnInfosByTableName(table.Name);
            Console.WriteLine($"Creating Table: {table.Name}");
            Console.WriteLine($"Columns: {JsonConvert.SerializeObject(columnInfos.Select(c => c.DbColumnName))}");

            foreach (var info in columnInfos.Where(col => col.IsPrimarykey && col.IsIdentity && col.DataType == "int"))
            {
                info.DataType = "integer"; // set datatype to integer, but sqlsugar may convert integer(10)
                info.Length = 0; // for remove (10) from sql
            }

            sqlite.DbMaintenance.CreateTable(table.Name, columnInfos);

            Console.WriteLine($@"Coping table: {table.Name}");
            Console.WriteLine($"Rows Count: {await mssql.Queryable<object>().AS(table.Name).CountAsync()}");
            var mssqlData = await mssql.Queryable<object>().AS(table.Name).ToDataTableAsync();
            await sqlite.Fastest<object>()
                .AS(table.Name)
                .BulkCopyAsync(mssqlData);
            Console.WriteLine();
        }
    }
}
