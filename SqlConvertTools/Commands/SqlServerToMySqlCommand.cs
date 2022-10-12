using System.Collections.Concurrent;
using System.CommandLine;
using System.Data;
using Microsoft.Data.SqlClient;
using MySql.Data.MySqlClient;
using SqlConvertTools.DbHandlers;

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
            "same as -user, but specify password. Ask user if has not available value");
        var sourcePasswordOption = new Option<string?>(new[] { "--source-password", "--sp" },
            "same as --source-user, but specify password and override --password. Ask user if has not available value");
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
                (string[])va(transferDatabase)!,
                (string?)vo(sourceUserNameOption), (string?)vo(targetUserNameOption),
                (string?)vo(passwordOption), (string?)vo(sourcePasswordOption), (string?)vo(targetPasswordOption),
                vo(ignoreTablesOption) as string[] ?? Array.Empty<string>(),
                vo(ignoreTablesForDatabasesOption) as IReadOnlyDictionary<string, IEnumerable<string>> ??
                new Dictionary<string, IEnumerable<string>>(),
                vo(customDatabaseNamesOption) as IReadOnlyDictionary<string, string> ??
                new Dictionary<string, string>());
        });
    }

    private static void Run(string sourceAddress, string targetAddress, string[] transferDatabase,
        string? sourceUserName, string? targetUserName,
        string? password, string? sourcePassword, string? targetPassword,
        string[] ignoreTables, IReadOnlyDictionary<string, IEnumerable<string>> ignoreDatabaseTables,
        IReadOnlyDictionary<string, string> customDatabaseNames)
    {
        var sourceConnStrBuilder = new SqlConnectionStringBuilder
        {
            DataSource = sourceAddress,
            UserID = sourceUserName ?? "sa",
            Password = sourcePassword ?? password ??
                throw new ArgumentException("has not available password for source sqlserver")
        };
        var targetConnStrBuilder = new MySqlConnectionStringBuilder
        {
            Server = targetAddress,
            UserID = targetUserName ?? "root",
            Password = targetPassword ?? password ??
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
                buildIgnoreTable.ToArray()).Wait();
        }
    }

    private static async Task TransferDatabase(string sourceConnectString, string targetConnectString,
        string[] ignoreTables)
    {
        using var sourceDb = new SqlserverHandler(sourceConnectString);
        using var targetDb = new MysqlHandler(targetConnectString);
        using var targetDb4 = new MysqlHandler(targetConnectString);
        targetDb4.TryConnect();

        sourceDb.BeforeFillNewTable += table =>
        {
            var tableName = table.TableName;
            //if (tableName is not "") continue;
            Console.WriteLine($"Creating Table: {tableName}");
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

            // ReSharper disable once AccessToDisposedClosure
            targetDb4.CreateTable(table);

            if (ignoreTables.Contains(tableName))
            {
                Console.WriteLine($"Ignored table: {tableName}, " +
                                  // ReSharper disable once AccessToDisposedClosure
                                  $"this will skip {sourceDb.GetRowCount(tableName)} row\n");
                return true;
            }

            Console.WriteLine($@"Coping table: {table.TableName}");
            // ReSharper disable once AccessToDisposedClosure
            Console.WriteLine($"Rows Count: {sourceDb.GetRowCount(tableName)}");

            Console.WriteLine();

            return false;
        };

        var queue = new ConcurrentQueue<DataRow>();
        var tokenSource = new CancellationTokenSource();
        sourceDb.TryConnect();
        targetDb.TryConnect();
        var fillTask = sourceDb.FillQueueAsync(queue, sourceDb.GetTableNames().ToArray(), tokenSource.Token);
        var peekTask = targetDb.PeekQueueAsync(queue, tokenSource.Token, CancellationToken.None);
        Task.WaitAny(fillTask, peekTask);
        tokenSource.Cancel();
        Task.WaitAll(fillTask, peekTask);
    }
}