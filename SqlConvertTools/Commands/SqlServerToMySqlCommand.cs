using System.CommandLine;
using System.Data;
using Microsoft.Data.SqlClient;
using SqlConvertTools.DbHandlers;
using MySqlConnectionStringBuilder = MySql.Data.MySqlClient.MySqlConnectionStringBuilder;

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

        this.SetHandler(async content =>
        {
            Func<Argument, object?> va = content.ParseResult.GetValueForArgument;
            Func<Option, object?> vo = content.ParseResult.GetValueForOption;
            await Run((string)va(sourceAddressArgument)!, (string)va(targetAddressArgument)!,
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

    private static async Task Run(string sourceAddress, string targetAddress, string[] transferDatabase,
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

            await TransferDatabase(sourceConnStrBuilder.ConnectionString, targetConnStrBuilder.ConnectionString,
                buildIgnoreTable.ToArray());
        }
    }

    private static async Task TransferDatabase(string sourceConnectString, string targetConnectString,
        string[] ignoreTables)
    {
        ignoreTables = ignoreTables.Select(i => i.ToLower()).ToArray();
        var sourceDb = new SqlserverHandler(sourceConnectString);
        var targetDb = new MysqlHandler(targetConnectString);
        targetDb.ConnectionStringBuilder.AllowLoadLocalInfile = true;

        var counter = new Dictionary<object, long>();
        var totalCount = 0L;

        targetDb.BulkCopyEvent += (sender, args) =>
        {
            lock (counter)
            {
                counter[sender] = args.RowsCopied;
                Logging.CurrentCount = counter.Values.Sum();
            }
        };

        var tokenSource = new CancellationTokenSource();
        var loggingTask = Logging.LogForCancel(tokenSource.Token);

        sourceDb.TryConnect();
        targetDb.TryConnect();
        var tasks = new List<Task>();
        var tables = sourceDb.GetTableNames().ToArray();
        foreach (var tblName in tables)
        {
            var reader = await sourceDb.CreateDataReader(tblName);
            var table = new DataTable(tblName);
            sourceDb.FillSchema(table);
            targetDb.CreateTable(table);

            #region Loggin

            Console.WriteLine($"Creating Table: {tblName}");
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

            var rowCount = sourceDb.GetRowCount(tblName);

            if (ignoreTables.Contains(tblName.ToLower()))
            {
                Console.WriteLine($"Ignored table: {tblName}, " +
                                  $"this will skip {rowCount} row\n");
                continue;
            }

            Console.WriteLine($@"Coping table: {table.TableName}");
            Console.WriteLine($"Rows Count: {rowCount:d4}");

            Console.WriteLine();

            #endregion

            if (rowCount is 0) continue;
            Logging.TotalCount += rowCount;

            while (tasks.Count(i => !(i.IsCompleted | i.IsCanceled | i.IsCompletedSuccessfully)) >= 3)
            {
                await Task.Delay(1000, tokenSource.Token);
            }

            if (tasks.Any(i => i.IsFaulted)) await Task.WhenAll(tasks);

            tasks.Add(targetDb.BulkCopy(tblName, reader));
        }

        await Task.WhenAll(tasks.ToArray());
        tokenSource.Cancel();
        try
        {
            await loggingTask;
        }
        catch (TaskCanceledException)
        {
        }
        Console.WriteLine($"\n\nSuccess transfer Database {targetDb.ConnectionStringBuilder.Database} for {totalCount} row");
    }
    private static class Logging
    {
        public static long TotalCount;
        public static long CurrentCount;
        private static long _prevCount;

        public static void LogOnce()
        {
            if (CurrentCount == _prevCount) return;
            Console.WriteLine($"{_prevCount}/{TotalCount} +{CurrentCount - _prevCount:d5}");
            Console.SetCursorPosition(0, Console.CursorTop - 1);
            _prevCount = CurrentCount;
        }

        public static async Task LogForCancel(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                LogOnce();
                await Task.Delay(100, token);
            }
        }
    }
}