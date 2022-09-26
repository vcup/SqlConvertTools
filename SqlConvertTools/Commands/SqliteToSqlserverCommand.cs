using System.CommandLine;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Data.Sqlite;
using Newtonsoft.Json;
using SqlSugar;

namespace SqlConvertTools.Commands;

public class SqliteToSqlserverCommand : Command
{
    public SqliteToSqlserverCommand()
        : base("sqlite2mssql", string.Empty)
    {
        var mssqlConnectStringArgument = new Argument<string>
        {
            Name = "mssql_conn_str",
            Description = "connect string of microsoft sql server"
        };

        var sqliteDbFileArgument = new Argument<FileInfo>
        {
            Name = "sqlite_path",
            Description = "database file path of sqlite",
        };

        var ignoreTablesOption = new Option<string[]>
        ("--ignore-tables", "ignore the given tables, but still create them");

        var sqlitePasswordOption = new Option<string>("--password", "password of sqlite if encrypted");

        AddArgument(sqliteDbFileArgument);
        AddArgument(mssqlConnectStringArgument);
        AddOption(ignoreTablesOption);
        AddOption(sqlitePasswordOption);

        this.SetHandler(Run, sqliteDbFileArgument, mssqlConnectStringArgument, ignoreTablesOption, sqlitePasswordOption);
    }

    private static async Task Run(FileSystemInfo sqliteDb, string mssqlConnString, string[] ignoreTables,
        string? password = null)
    {
        var sqliteConnectStringBuilder = new SqliteConnectionStringBuilder
        {
            DataSource = sqliteDb.FullName,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Shared,
        };
        if (password is not null)
        {
            sqliteConnectStringBuilder.Password = password;
        }

        using var sqlite = new SqlSugarClient(new ConnectionConfig
        {
            ConnectionString = sqliteConnectStringBuilder.ToString(),
            IsAutoCloseConnection = true,
            DbType = DbType.Sqlite,
            ConfigId = "sqlite"
        });
        using var mssql = new SqlSugarClient(new ConnectionConfig
        {
            ConnectionString = mssqlConnString,
            DbType = DbType.SqlServer,
            IsAutoCloseConnection = true,
            ConfigId = "mssql"
        });

        mssql.DbMaintenance.CreateDatabase();

        var tables = sqlite.DbMaintenance
            .GetTableInfoList(false);
        foreach (var table in tables)
        {
            var columnInfos = sqlite.DbMaintenance.GetColumnInfosByTableName(table.Name);
            Console.WriteLine($"Creating Table: {table.Name}");
            Console.WriteLine($"Columns: {JsonConvert.SerializeObject(columnInfos.Select(c => c.DbColumnName))}");

            ConvertDataType(columnInfos);

            if (mssql.DbMaintenance.IsAnyTable(table.Name, false)) mssql.DbMaintenance.DropTable(table.Name);
            mssql.DbMaintenance.CreateTable(table.Name, columnInfos);

            var rowCount = await sqlite.Queryable<object>().AS(table.Name).CountAsync();
            if (ignoreTables.Any(tbl => tbl == table.Name))
            {
                Console.WriteLine($"Ignored table: {table.Name}, this will skip {rowCount} row\n");
                continue;
            }

            Console.WriteLine($@"Coping table: {table.Name}");
            Console.WriteLine($"Rows Count: {rowCount}");
            var sqliteData = await sqlite.Queryable<object>().AS(table.Name).ToDataTableAsync();
            await mssql.Fastest<object>()
                .AS(table.Name)
                .BulkCopyAsync(sqliteData);
            Console.WriteLine();
        }
    }

    private static void ConvertDataType(IEnumerable<DbColumnInfo> infos)
    {
        foreach (var info in infos)
        {
            switch (info.DataType)
            {
                case "int":
                case "tinyint":
                case "smallint":
                case "bigint":
                case "bit":
                case "date":
                case "datetime":
                case "timestamp":
                case "uniqueidentifier":
                case "text":
                case "ntext":
                    info.Length = 0;
                    info.DecimalDigits = 0;
                    break; // remove there trailing length limit, e.g. $"{info.DataType}(length, decimalDigits)"
                case "varchar":
                case "nvarchar":
                    if (info.Length <= 0)
                    {
                        info.Length = 4001;
                    }

                    // for solve datatype want to MAX but doest, see https://www.donet5.com/Ask/9/16701
                    break; // let datatype finally equal nvarchar(max)
            }
        }
    }
}
