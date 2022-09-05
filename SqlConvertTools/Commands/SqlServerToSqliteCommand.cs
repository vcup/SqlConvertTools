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

        var ignoreTablesOption = new Option<string[]>(
            "--ignore-tables",
            "ignore the given tables, but still create them");

        AddArgument(mssqlConnectStringArgument);
        AddArgument(sqliteDbFileArgument);

        this.SetHandler(Run, mssqlConnectStringArgument, sqliteDbFileArgument, ignoreTablesOption);
    }

    private static async Task Run(string mssqlConnString, FileSystemInfo? sqliteDb, string[] ignoreTables)
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

            foreach (var col in columnInfos.Where(col => col.IsIdentity && col.DataType == "int"))
            {
                col.DataType = "integer"; // set datatype to integer, but sqlsugar may convert integer(10)
                col.Length = 0; // for remove (10) from sql
                col.IsPrimarykey = true;
            }

            sqlite.DbMaintenance.CreateTable(table.Name, columnInfos);

            var rowCount = await mssql.Queryable<object>().AS(table.Name).CountAsync();
            if (ignoreTables.Any(tbl => tbl == table.Name))
            {
                Console.WriteLine($"Ignore this table: {table.Name}, this will skip {rowCount} row");
            }

            Console.WriteLine($@"Coping table: {table.Name}");
            Console.WriteLine($"Rows Count: {rowCount}");
            var mssqlData = await mssql.Queryable<object>().AS(table.Name).ToDataTableAsync();
            await sqlite.Fastest<object>()
                .AS(table.Name)
                .BulkCopyAsync(mssqlData);
            Console.WriteLine();
        }
    }
}
