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

        var ignoreTablesOption = new Option<string[]>
            ("--ignore-tables", "ignore the given tables, but still create them");
        var ignoreTablesForDatabasesOption = new Option<IReadOnlyDictionary<string, IEnumerable<string>>>(
            "--ignore-database-tables",
            result =>
            {
                var parseResult = new Dictionary<string, IEnumerable<string>>();
                var parsingSplit = result.Tokens
                    .Select(i => i.Value.Split(':'))
                    .Select(i => (i[0], i[1..]));
                foreach (var (key, value) in parsingSplit)
                {
                    if (parseResult.TryGetValue(key, out var list))
                    {
                        (list as List<string>)!.AddRange(value);
                    }
                    else
                    {
                        parseResult[key] = new List<string>(value);
                    }
                }

                return parseResult;
            })
        {
            Description = "ignore the given tables, but still create them.\n" +
                          "example -> dbname:tblName:tblName1 => dbname: [tblName, tblName1]",
            Arity = ArgumentArity.ZeroOrMore,
        };

        AddArgument(sourceAddressArgument);
        AddArgument(targetAddressArgument);
        AddArgument(transferDatabase);
        AddOption(userNameOption);
        AddOption(sourceUserNameOption);
        AddOption(targetUserNameOption);
        AddOption(passwordOption);
        AddOption(sourcePasswordOption);
        AddOption(targetPasswordOption);
        AddOption(ignoreTablesOption);
        AddOption(ignoreTablesForDatabasesOption);

        this.SetHandler(async content =>
        {
            Func<Argument, object?> va = content.ParseResult.GetValueForArgument;
            Func<Option, object?> vo = content.ParseResult.GetValueForOption;
            await Run((string)va(sourceAddressArgument)!, (string)va(targetAddressArgument)!,
                (string[])va(transferDatabase)!,
                (string?)vo(userNameOption), (string?)vo(sourceUserNameOption), (string?)vo(targetUserNameOption),
                (string?)vo(passwordOption), (string?)vo(sourcePasswordOption), (string?)vo(targetPasswordOption),
                vo(ignoreTablesOption) as string[] ?? Array.Empty<string>(),
                (Dictionary<string, IEnumerable<string>>)vo(ignoreTablesForDatabasesOption)!
            );
        });
    }

    private static async Task Run(string sourceAddress, string targetAddress, string[] transferDatabase,
        string? userName, string? sourceUserName, string? targetUserName,
        string? password, string? sourcePassword, string? targetPassword,
        string[] ignoreTables, IReadOnlyDictionary<string, IEnumerable<string>> ignoreDatabaseTables)
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
                .Select(r => (string)r.name)
                .ToArray();
            // db.DbMaintenance.GetDataBaseList(db); do not ignore system database
        }

        foreach (var dbname in transferDatabase)
        {
            sourceConnStrBuilder.InitialCatalog = targetConnStrBuilder.InitialCatalog = dbname;
            IEnumerable<string> buildIgnoreTable = ignoreTables;
            if (ignoreDatabaseTables.TryGetValue(dbname, out var item))
            {
                buildIgnoreTable = ignoreTables.Concat(item);
            }

            await TransferDatabase(sourceConnStrBuilder.ConnectionString, targetConnStrBuilder.ConnectionString,
                buildIgnoreTable.ToArray());
        }
    }

    private static async Task TransferDatabase(string sourceConnectString, string targetConnectString,
        string[] ignoreTables)
    {
        using var sourceDb = new SqlSugarClient(new ConnectionConfig
        {
            ConnectionString = sourceConnectString,
            DbType = DbType.SqlServer,
            IsAutoCloseConnection = true,
            ConfigId = "source"
        });
        using var targetDb = new SqlSugarClient(new ConnectionConfig
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
            //if (table.Name is not "") continue;
            table.Name = $"[{table.Name}]";
            // fix: some table name may contain .(dot), may convert to [table_name_part1].[table_name_part2]
            var columnInfos = sourceDb.DbMaintenance.GetColumnInfosByTableName(table.Name);
            Console.WriteLine($"Creating Table: {table.Name}");
            Console.WriteLine($"Columns: {JsonConvert.SerializeObject(columnInfos.Select(c => c.DbColumnName))}");

            ConvertDataType(columnInfos);

            if (targetDb.DbMaintenance.IsAnyTable(table.Name, false)) targetDb.DbMaintenance.DropTable(table.Name);
            targetDb.DbMaintenance.CreateTable(table.Name, columnInfos);

            var rowCount = await sourceDb.Queryable<object>().AS(table.Name).CountAsync();
            if (ignoreTables.Any(tbl => tbl == table.Name))
            {
                Console.WriteLine($"Ignored table: {table.Name}, this will skip {rowCount} row\n");
                continue;
            }

            Console.WriteLine($@"Coping table: {table.Name}");
            Console.WriteLine($"Rows Count: {rowCount}");
            var dataTable = await sourceDb.Queryable<object>().AS(table.Name).ToDataTableAsync();
            await targetDb.Fastest<object>()
                .AS(table.Name)
                .BulkCopyAsync(dataTable);
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
                    if (info.Length == -1)
                    {
                        info.Length = 4001;
                    }

                    // for solve datatype want to MAX but doest, see https://www.donet5.com/Ask/9/16701
                    break; // let datatype finally equal nvarchar(max)
            }
        }
    }
}