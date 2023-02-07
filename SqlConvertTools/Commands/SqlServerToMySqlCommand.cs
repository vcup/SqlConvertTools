using System.Collections.Concurrent;
using System.CommandLine;
using System.Data;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using SqlConvertTools.DbHandlers;
using SqlConvertTools.Helper;

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
            Description = "specify database for transfer\n" +
                          "use ':' to specify what table will be create and transfer\n" +
                          "example: dbname:tbl_1:tbl_2 -> only tbl_1 & tbl_2 will create and transfer\n" +
                          "only specify dbname will transfer all table of db"
        };
        var sourceUserNameOption = new Option<string?>(new[] { "--source-user", "--su" },
            "specify transfer task to used user name for source sqlserver, will override --user");
        var targetUserNameOption =
            new Option<string?>(new[] { "--target-user", "--tu" },
                "same as --source-user, but is for target mysql server");
        var passwordOption = new Option<string?>(new[] { "--password", "-p" },
            "same as -user, but specify password. Ask user if has not available value");
        var sourcePasswordOption = new Option<string?>(new[] { "--source-password", "--sp" },
            "same as --source-user, but specify password and override --password. Ask user if has not available value[TODO]");
        var targetPasswordOption = new Option<string?>(new[] { "--target-password", "--tp" },
            "same as --source-password and similar to --target-user");

        var ignoreTablesOption = new Option<string[]>
            ("--ignore-tables", "ignore the given tables, but still create them")
            {
                AllowMultipleArgumentsPerToken = true,
            };
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
            AllowMultipleArgumentsPerToken = true,
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
            Description = "Transfer source database to a custom name. Doesn't affect for --ignore-database-tables\n" +
                          "format -> source_name:dest_name",
            Arity = ArgumentArity.ZeroOrMore,
            AllowMultipleArgumentsPerToken = true,
        };

        var trustSourceOption = new Option<bool>("--trust-source-cert");
        var overrideTableIfExistOption = new Option<bool>
            ("--override-table", "override table if already exist, it will recreate and transfer");

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
        AddOption(trustSourceOption);
        AddOption(overrideTableIfExistOption);

        this.SetHandler(async content =>
        {
            await Run(Va(sourceAddressArgument)!, Va(targetAddressArgument)!,
                Va(transferDatabase)!,
                Vo(sourceUserNameOption), Vo(targetUserNameOption),
                Vo(passwordOption), Vo(sourcePasswordOption), Vo(targetPasswordOption),
                Vo(ignoreTablesOption) ?? Array.Empty<string>(),
                Vo(ignoreTablesForDatabasesOption) ??
                new Dictionary<string, IEnumerable<string>>(),
                Vo(customDatabaseNamesOption) ??
                new Dictionary<string, string>(), Vo(trustSourceOption), Vo(overrideTableIfExistOption));

            T? Va<T>(Argument<T> o) => content.ParseResult.GetValueForArgument(o);
            T? Vo<T>(Option<T> o) => content.ParseResult.GetValueForOption(o);
        });
    }

    private static async Task Run(string sourceAddress, string targetAddress, string[] transferDatabase,
        string? sourceUserName, string? targetUserName,
        string? password, string? sourcePassword, string? targetPassword,
        string[] ignoreTables, IReadOnlyDictionary<string, IEnumerable<string>> ignoreDatabaseTables,
        IReadOnlyDictionary<string, string> customDatabaseNames, bool trustSourceCert, bool overrideTableIfExist)
    {
        var sourceConnStrBuilder = new SqlConnectionStringBuilder
        {
            DataSource = sourceAddress,
            UserID = sourceUserName ?? "sa",
            Password = sourcePassword ?? password ??
                throw new ArgumentException("has not available password for source sqlserver")
        };
        if (trustSourceCert) sourceConnStrBuilder.TrustServerCertificate = true;
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

        foreach (var transferDb in transferDatabase)
        {
            var dbAndTable = transferDb.Split(':');
            var (dbname, tblNames) = (dbAndTable[0], dbAndTable[1..]);
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
                buildIgnoreTable.ToArray(), tblNames, overrideTableIfExist);
        }
    }

    private static async Task TransferDatabase(string sourceConnectString, string targetConnectString,
        string[] ignoreTables, IReadOnlyCollection<string> tblNames, bool overrideTableIfExist)
    {
        ignoreTables = ignoreTables.Select(i => i.ToLower()).ToArray();
        var sourceDb = new SqlserverHandler(sourceConnectString);
        var targetDb = new MysqlHandler(targetConnectString);
        targetDb.ConnectionStringBuilder.AllowLoadLocalInfile = true;

        {
            if (!sourceDb.TryConnect(out var e)) throw e;
        }
        {
            if (!targetDb.TryConnect(out var e)) throw e;
        }

        var tables = tblNames.Count is 0
            ? sourceDb.GetTableNames().ToArray()
            : sourceDb.GetTableNames()
                .Intersect(tblNames, StringComparer.OrdinalIgnoreCase)
                .ToArray();
        if (overrideTableIfExist)
        {
            var overrideTables = targetDb.GetTableNames()
                .Intersect(tables, StringComparer.OrdinalIgnoreCase)
                .ToArray();
            if (overrideTables.Length is not 0)
            {
                Console.WriteLine("showing below tables will be override:");
                foreach (var overrideTable in overrideTables)
                {
                    Console.WriteLine($"- {overrideTable}");
                }

                Console.WriteLine("did you sure? (yes/no)");
                if (Console.ReadLine()?.ToLower() is not "yes") return;
            }
        }

        var tokenSource = new CancellationTokenSource();
        var loggingTask = LoggingHelper.LogForCancel(tokenSource.Token);

        var totalCount = 0L;
        var counter = new ConcurrentDictionary<object, long>();
        // collect copied rows count
        targetDb.BulkCopyEvent += (sender, args) =>
        {
            counter[sender] = args.RowsCopied;
            LoggingHelper.CurrentCount = counter.Values.Sum();
        };

        var tasks = new List<Task>();
        foreach (var tblName in tables)
        {
            var table = new DataTable(tblName);
            sourceDb.FillSchema(table);
            targetDb.CreateTable(table, overrideTableIfExist);

            var rowCount = sourceDb.GetRowCount(tblName);
            await LoggingHelper.LogTables(tblName, table, ignoreTables, rowCount);

            if (rowCount is 0 || ignoreTables.Contains(tblName, StringComparer.OrdinalIgnoreCase)) continue;
            totalCount += LoggingHelper.TotalCount += rowCount;

            if (tasks.Any(i => i.IsFaulted)) await Task.WhenAll(tasks);

            var reader = await sourceDb.CreateDataReader(tblName);
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

        Console.WriteLine("Success transfer Database " +
                          $"{targetDb.ConnectionStringBuilder.Database} for {totalCount} row");
    }
}