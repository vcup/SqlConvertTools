using System.Collections.Concurrent;
using System.CommandLine;
using System.Data;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using SqlConvertTools.DbHandlers;
using SqlConvertTools.Helper;
using SqlConvertTools.Utils;
using static SqlConvertTools.Helper.ParsedOptions;

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
        var parallelTablesTransferOption = new Option<int>
            ("--parallel-table-transfers", () => 3)
            {
                Description = "specify how many tables transfer in same time"
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
        AddOption(trustSourceOption);
        AddOption(overrideTableIfExistOption);
        AddOption(parallelTablesTransferOption);
        AddOption(customColumnDataTypeOption);

        this.SetHandler(async content =>
        {
            SourceUserName = Vo(sourceUserNameOption);
            TargetUserName = Vo(targetUserNameOption);
            Password = Vo(passwordOption);
            SourcePassword = Vo(sourcePasswordOption);
            TargetPassword = Vo(targetPasswordOption);
            IgnoreTables = Vo(ignoreTablesOption) ?? Array.Empty<string>();
            IgnoreDatabaseTables = Vo(ignoreTablesForDatabasesOption) ??
                                                 new Dictionary<string, IEnumerable<string>>();
            CustomDatabaseNames = Vo(customDatabaseNamesOption) ?? new Dictionary<string, string>();
            TrustSourceCert = Vo(trustSourceOption);
            OverrideTableIfExist = Vo(overrideTableIfExistOption);
            ParallelTablesTransfer = Vo(parallelTablesTransferOption);

            await Run(Va(sourceAddressArgument)!, Va(targetAddressArgument)!,
                Va(transferDatabase)!);

            T? Va<T>(Argument<T> o) => content.ParseResult.GetValueForArgument(o);
            T? Vo<T>(Option<T> o) => content.ParseResult.GetValueForOption(o);
        });
    }

    private static async Task Run(string sourceAddress, string targetAddress, string[] transferDatabase)
    {
        var sourceConnStrBuilder = new SqlConnectionStringBuilder
        {
            DataSource = sourceAddress,
            UserID = SourceUserName ?? "sa",
            Password = SourcePassword ?? Password ??
                throw new ArgumentException("has not available password for source sqlserver")
        };
        if (TrustSourceCert) sourceConnStrBuilder.TrustServerCertificate = true;
        var targetConnStrBuilder = new MySqlConnectionStringBuilder
        {
            Server = targetAddress,
            UserID = TargetUserName ?? "root",
            Password = TargetPassword ?? Password ??
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
            const RegexOptions dbTblRegexOptions =
                RegexOptions.Compiled
                | RegexOptions.Singleline
                | RegexOptions.IgnoreCase
                | RegexOptions.CultureInvariant;
            var dbAndTable = transferDb.Replace("(?:", "(?$%$", StringComparison.Ordinal)
                .Split(':', StringSplitOptions.RemoveEmptyEntries);
            var (dbname, tblNameMatchers) = (dbAndTable[0], dbAndTable.Skip(1)
                .Select(i => i.Replace("(?$%$", "(?:", StringComparison.Ordinal))
                .Select(i => new Regex(i, dbTblRegexOptions))
                .ToArray());
            sourceConnStrBuilder.InitialCatalog = targetConnStrBuilder.Database = dbname;
            if (CustomDatabaseNames.TryGetValue(dbname, out var customDbName))
            {
                targetConnStrBuilder.Database = customDbName;
            }

            IEnumerable<string> buildIgnoreTable = IgnoreTables;
            if (IgnoreDatabaseTables.TryGetValue(dbname, out var item))
            {
                buildIgnoreTable = IgnoreTables.Concat(item);
            }

            await TransferDatabase(sourceConnStrBuilder.ConnectionString, targetConnStrBuilder.ConnectionString,
                buildIgnoreTable.ToArray(), tblNameMatchers);
        }
    }

    private static async Task TransferDatabase(string sourceConnectString, string targetConnectString,
        string[] ignoreTables, IReadOnlyCollection<Regex> tblNameMatchers)
    {
        ignoreTables = ignoreTables.Select(i => i.ToLower()).ToArray();
        var sourceDb = new SqlserverHandler(sourceConnectString);
        var targetDb = new MysqlHandler(targetConnectString);
        // bulk copy required
        targetDb.ConnectionStringBuilder.AllowLoadLocalInfile = true;

        {
            if (!sourceDb.TryConnect(out var e)) throw e;
        }
        {
            if (!targetDb.TryConnect(out var e)) throw e;
        }

        var tables = tblNameMatchers.Count is 0
            ? sourceDb.GetTableNames().ToArray()
            : sourceDb.GetTableNames()
                .Where(i => tblNameMatchers.Any(j => j.IsMatch(i)))
                .ToArray();
        if (OverrideTableIfExist)
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
            counter[sender] = args.EventArguments!.RowsCopied;
            LoggingHelper.CurrentCount = counter.Values.Sum();
            LoggingHelper.CurrentTableName = args.TableName;
        };

        var tasks = new List<Task>();
        foreach (var tblName in tables)
        {
            var table = new DataTable(tblName);
            sourceDb.FillSchema(table);
            targetDb.CreateTable(table, OverrideTableIfExist);

            var rowCount = sourceDb.GetRowCount(tblName);
            await LoggingHelper.LogTables(tblName, table, ignoreTables, rowCount);

            if (rowCount is 0 || ignoreTables.Contains(tblName, StringComparer.OrdinalIgnoreCase)) continue;
            totalCount += LoggingHelper.TotalCount += rowCount;

            if (tasks.Any(i => i.IsFaulted)) await Task.WhenAll(tasks);

            var reader = await sourceDb.CreateDataReader(tblName);
            LoggingHelper.InCompleteTasks = tasks.Where(i => !i.IsCompleted).ToArray();
            if (LoggingHelper.InCompleteTasks.Length == ParallelTablesTransfer) await Task.WhenAny(LoggingHelper.InCompleteTasks);
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