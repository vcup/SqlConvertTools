using System.CommandLine;
using System.Data;
using Microsoft.Data.SqlClient;
using MySql.Data.MySqlClient;
using SqlConvertTools.DbHandlers;
using SqlConvertTools.Extensions;

namespace SqlConvertTools.Commands;

public class SqlServerToMySqlCommand : Command
{
    public SqlServerToMySqlCommand() : base("mssql2mysql")
    {
        var sourceAddressArgument = new Argument<string>
        {
            Name = "source_server_address",
            Description = "source sqlserver address of want to transfer data"
        };
        var targetAddressArgument = new Argument<string>
        {
            Name = "target_server_address",
            Description = "target mysql server address of want to transfer data"
        };
        var transferDatabase = new Argument<string[]>(Array.Empty<string>)
        {
            Name = "transfer_database",
            Description = "specify database for transfer"
        };
        var sourceUserNameOption = new Option<string?>(new[] { "--source-user", "--su" },
            "specify transfer task to used user name for source sqlserver, will override --user");
        var targetUserNameOption =
            new Option<string?>(new[] { "--target-user", "--tu" },
                "same as --source-user, but is for target mysql server");
        var passwordOption = new Option<string?>(new[] { "--password", "-p" },
            "same as -user, but specify password.");
        var sourcePasswordOption = new Option<string?>(new[] { "--source-password", "--sp" },
            "same as --source-user, but specify password and override --password.");
        var targetPasswordOption = new Option<string?>(new[] { "--target-password", "--tp" },
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

        var customDatabaseNamesOption = new Option<IReadOnlyDictionary<string, string>>(
            "--custom-database-names",
            result => new Dictionary<string, string>(result.Tokens
                .Select(i => i.Value.Split(':'))
                .Select(i => (i[0], i[1]))
                .Select(i => new KeyValuePair<string, string>(i.Item1, i.Item2))
            )
        )
        {
            Description = "Transfer source database as a custom name. Doest affect --ignore-database-tables\n" +
                          "format -> source_name:dest_name",
            Arity = ArgumentArity.ZeroOrMore,
        };

        AddArgument(sourceAddressArgument);
        AddArgument(targetAddressArgument);
        AddArgument(transferDatabase);
        AddOption(sourceUserNameOption);
        AddOption(targetUserNameOption);
        AddOption(passwordOption);
        AddOption(sourcePasswordOption);
        AddOption(targetPasswordOption);
        AddOption(ignoreTablesOption);
        AddOption(ignoreTablesForDatabasesOption);
        AddOption(customDatabaseNamesOption);

        this.SetHandler(content =>
        {
            Func<Argument, object?> va = content.ParseResult.GetValueForArgument;
            Func<Option, object?> vo = content.ParseResult.GetValueForOption;
            Run((string)va(sourceAddressArgument)!, (string)va(targetAddressArgument)!,
                (string[])va(transferDatabase)!, (string?)vo(sourceUserNameOption), (string?)vo(targetUserNameOption),
                (string?)vo(passwordOption), (string?)vo(sourcePasswordOption), (string?)vo(targetPasswordOption),
                vo(ignoreTablesOption) as string[] ?? Array.Empty<string>(),
                vo(ignoreTablesForDatabasesOption) as IReadOnlyDictionary<string, IEnumerable<string>> ??
                new Dictionary<string, IEnumerable<string>>(),
                vo(customDatabaseNamesOption) as IReadOnlyDictionary<string, string> ??
                new Dictionary<string, string>());
        });
    }

    private static void Run(string sourceAddress, string targetAddress, string[] transferDatabase,
        string? userName, string? sourceUserName, string? targetUserName,
        string? sourcePassword, string? targetPassword,
        string[] ignoreTables, IReadOnlyDictionary<string, IEnumerable<string>> ignoreDatabaseTables,
        IReadOnlyDictionary<string, string> customDatabaseNames)
    {
        var sourceConnStrBuilder = new SqlConnectionStringBuilder
        {
            DataSource = sourceAddress,
            UserID = sourceUserName ?? userName ?? "sa",
            Password = sourcePassword ??
                       throw new ArgumentException("has not available password for source sqlserver")
        };
        var targetConnStrBuilder = new MySqlConnectionStringBuilder
        {
            Server = targetAddress,
            UserID = targetUserName ?? userName ?? "root",
            Password = targetPassword ??
                       throw new ArgumentException("has not available password for target sqlserver")
        };
        if (!transferDatabase.Any())
        {
            sourceConnStrBuilder.InitialCatalog = "master";
            using var dbHandler = new SqlserverHandler(sourceConnStrBuilder);
            dbHandler.TryConnect();
            transferDatabase = dbHandler.GetDatabases().ToArray();
        }

        foreach (var dbname in transferDatabase)
        {
            sourceConnStrBuilder.InitialCatalog = targetConnStrBuilder.Database = dbname;
            if (customDatabaseNames.TryGetValue(dbname, out var customDbName))
            {
                targetConnStrBuilder.Database = customDbName;
            }

            IEnumerable<string> buildIgnoreTable = ignoreTables;
            if (ignoreDatabaseTables.TryGetValue(dbname, out var item))
            {
                buildIgnoreTable = ignoreTables.Concat(item);
            }

            TransferDatabase(sourceConnStrBuilder.ConnectionString, targetConnStrBuilder.ConnectionString,
                buildIgnoreTable.ToArray());
        }
    }

    private static void TransferDatabase(string sourceConnectString, string targetConnectString,
        string[] ignoreTables)
    {
        using var sourceDb = new SqlserverHandler(sourceConnectString);
        using var targetDb = new MysqlHandler(targetConnectString);

        targetDb.TryConnect();

        var dataSet = new DataSet();

        sourceDb.TryConnect();
        foreach (var tableName in sourceDb.GetTableNames().ToArray())
        {
            //if (tableName is not "") continue;
            Console.WriteLine($"Creating Table: {tableName}");
            sourceDb.FillDatasetWithoutData(tableName, dataSet);
            var table = dataSet.Tables[tableName] ?? throw new NullReferenceException();
            Console.Write("Columns: ");
            for (var i = 0;;)
            {
                Console.Write('[' + table.Columns[i].ColumnName + ']');
                if (++i < table.Columns.Count)
                {
                    Console.Write(',');
                    continue;
                }

                break;
            }

            Console.WriteLine();

            targetDb.CreateTable(table);

            if (ignoreTables.Contains(tableName))
            {
                Console.WriteLine(
                    $"Ignored table: {tableName}, this will skip {sourceDb.GetRowCount(tableName)} row\n");
                continue;
            }

            Console.WriteLine($@"Coping table: {tableName}");
            Console.WriteLine($"Rows Count: {sourceDb.GetRowCount(tableName)}");

            sourceDb.FillDataset(tableName, dataSet, out _);
            if (table.Rows.Count is not 0)
            {
                foreach (DataRow row in table.Rows)
                {
                    row.SetState(DataRowState.Added);
                }
            }

            targetDb.UpdateDatabaseWith(table);
            Console.WriteLine();
        }
    }
}