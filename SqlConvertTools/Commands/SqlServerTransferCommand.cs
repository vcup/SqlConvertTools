using System.CommandLine;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
using SqlSugar;

namespace SqlConvertTools.Commands;

public class SqlServerTransferCommand : Command
{
    public SqlServerTransferCommand()
        : base("mssql-transfer")
    {
        var sourceAddressArgument = new Argument<string>
        {
            Name = "source_server_address",
            Description = "source sqlserver address of want to transfer data"
        };
        var targetAddressArgument = new Argument<string>
        {
            Name = "target_server_address",
            Description = "target sqlserver address of want to transfer data"
        };
        var transferDatabase = new Argument<string[]>(Array.Empty<string>)
        {
            Name = "transfer_database",
            Description = "specify database for transfer"
        };
        var userNameOption = new Option<string?>(new[] { "--user", "-u" }, 
            "specify transfer task to used user name, use --source-user or --target-user specify value for sqlserver individually. if has not available value, will use 'sa'");
        var sourceUserNameOption = new Option<string?>("--source-user",
            "specify transfer task to used user name for source sqlserver, will override --user");
        var targetUserNameOption =
            new Option<string?>("--target-user", "same as --source-user, but is for target sqlserver");
        var passwordOption = new Option<string?>(new[] { "--password", "-p" }, 
            "same as -user, but specify password. Ask user if has not available value");
        var sourcePasswordOption = new Option<string?>("--source-password",
            "same as --source-user, but specify password and override --password. Ask user if has not available value");
        var targetPasswordOption = new Option<string?>("--target-password",
            "same as --source-password and similar to --target-user");

        AddArgument(sourceAddressArgument);
        AddArgument(targetAddressArgument);
        AddArgument(transferDatabase);
        AddOption(userNameOption);
        AddOption(sourceUserNameOption);
        AddOption(targetUserNameOption);
        AddOption(passwordOption);
        AddOption(sourcePasswordOption);
        AddOption(targetPasswordOption);

        this.SetHandler(async content =>
        {
            Func<Argument, object?> va = content.ParseResult.GetValueForArgument;
            Func<Option, object?> vo = content.ParseResult.GetValueForOption;
            await Run((string)va(sourceAddressArgument)!, (string)va(targetAddressArgument)!,
                (string[])va(transferDatabase)!,
                (string?)vo(userNameOption), (string?)vo(sourceUserNameOption), (string?)vo(targetUserNameOption),
                (string?)vo(passwordOption), (string?)vo(sourcePasswordOption), (string?)vo(targetPasswordOption)
            );
        });
    }

    private static async Task Run(string sourceAddress, string targetAddress, string[] transferDatabase,
        string? userName, string? sourceUserName, string? targetUserName,
        string? password, string? sourcePassword, string? targetPassword)
    {
        var sourceConnStrBuilder = new SqlConnectionStringBuilder
        {
            DataSource = sourceAddress,
            UserID = sourceUserName ?? userName ?? "sa",
            Password = sourcePassword ??
                       password ?? throw new ArgumentException("has not available password for source sqlserver")
        };
        var targetConnStrBuilder = new SqlConnectionStringBuilder
        {
            DataSource = targetAddress,
            UserID = targetUserName ?? userName ?? "sa",
            Password = targetPassword ?? password ??
                throw new ArgumentException("has not available password for target sqlserver")
        };
        if (!transferDatabase.Any())
        {
            sourceConnStrBuilder.InitialCatalog = "master";
            var db = new SqlSugarClient(new ConnectionConfig
            {
                ConnectionString = sourceConnStrBuilder.ConnectionString,
                DbType = DbType.SqlServer,
                IsAutoCloseConnection = true
            });
            transferDatabase =
                (await db.SqlQueryable<dynamic>("select name from sys.databases where database_id > 4").ToArrayAsync())
                .Select(r => (string)r.name).ToArray();
            // db.DbMaintenance.GetDataBaseList(db); do not ignore system database
        }

        foreach (var dbname in transferDatabase)
        {
            sourceConnStrBuilder.InitialCatalog = targetConnStrBuilder.InitialCatalog = dbname;
            await TransferDatabase(sourceConnStrBuilder.ConnectionString, targetConnStrBuilder.ConnectionString);
        }
    }

    private static async Task TransferDatabase(string sourceConnectString, string targetConnectString)
    {
        var sourceDb = new SqlSugarClient(new ConnectionConfig
        {
            ConnectionString = sourceConnectString,
            DbType = DbType.SqlServer,
            IsAutoCloseConnection = true,
            ConfigId = "source"
        });
        var targetDb = new SqlSugarClient(new ConnectionConfig
        {
            ConnectionString = targetConnectString,
            IsAutoCloseConnection = true,
            DbType = DbType.SqlServer,
            ConfigId = "target"
        });

        targetDb.DbMaintenance.CreateDatabase();

        var tables = sourceDb.DbMaintenance
            .GetTableInfoList(false);
        foreach (var table in tables)
        {
            var columnInfos = sourceDb.DbMaintenance.GetColumnInfosByTableName(table.Name);
            Console.WriteLine($"Creating Table: {table.Name}");
            Console.WriteLine($"Columns: {JsonConvert.SerializeObject(columnInfos.Select(c => c.DbColumnName))}");

            foreach (var info in columnInfos)
            {
                switch (info.DataType)
                {
                    case "int":
                    case "bit":
                    case "date":
                    case "datetime":
                    case "uniqueidentifier":
                        info.Length = 0;
                        info.DecimalDigits = 0;
                        break; // remove there trailing length limit, e.g. (length, decimalDigits)
                    case "varchar":
                    case "nvarchar":
                        info.Length = 4001;
                        break; // let datatype finally equal nvarchar(max)
                }
            }

            if (targetDb.DbMaintenance.IsAnyTable(table.Name, false)) targetDb.DbMaintenance.DropTable(table.Name);
            targetDb.DbMaintenance.CreateTable(table.Name, columnInfos);

            var rowCount = await sourceDb.Queryable<object>().AS(table.Name).CountAsync();

            Console.WriteLine($@"Coping table: {table.Name}");
            Console.WriteLine($"Rows Count: {rowCount}");
            var dataTable = await sourceDb.Queryable<object>().AS(table.Name).ToDataTableAsync();
            await targetDb.Fastest<object>()
                .AS(table.Name)
                .BulkCopyAsync(dataTable);
            Console.WriteLine();
        }
    }
}