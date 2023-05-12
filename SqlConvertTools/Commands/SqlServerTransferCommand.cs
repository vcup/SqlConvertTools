using System.CommandLine;
using System.Data;
using Microsoft.Data.SqlClient;
using SqlConvertTools.DbHandlers;
using SqlConvertTools.Extensions;
using SqlConvertTools.Helper;

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
        AddOption(userNameOption);
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
                (string?)vo(userNameOption), (string?)vo(sourceUserNameOption), (string?)vo(targetUserNameOption),
                (string?)vo(passwordOption), (string?)vo(sourcePasswordOption), (string?)vo(targetPasswordOption),
                vo(ignoreTablesOption) as string[] ?? Array.Empty<string>(),
                vo(ignoreTablesForDatabasesOption) as IReadOnlyDictionary<string, IEnumerable<string>> ??
                new Dictionary<string, IEnumerable<string>>(),
                vo(customDatabaseNamesOption) as IReadOnlyDictionary<string, string> ??
                new Dictionary<string, string>());
        });
    }

    private static async Task Run(string sourceAddress, string targetAddress, string[] transferDatabase,
        string? userName, string? sourceUserName, string? targetUserName,
        string? password, string? sourcePassword, string? targetPassword,
        string[] ignoreTables, IReadOnlyDictionary<string, IEnumerable<string>> ignoreDatabaseTables,
        IReadOnlyDictionary<string, string> customDatabaseNames)
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
            using var dbHandler = new SqlserverHandler(sourceConnStrBuilder);
            dbHandler.TryConnect();
            transferDatabase = dbHandler.GetDatabases().ToArray();
        }

        foreach (var dbname in transferDatabase)
        {
            sourceConnStrBuilder.InitialCatalog = targetConnStrBuilder.InitialCatalog = dbname;
            if (customDatabaseNames.TryGetValue(dbname, out var customDbName))
            {
                targetConnStrBuilder.InitialCatalog = customDbName;
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
        using var sourceDb = new SqlserverHandler(sourceConnectString);
        using var targetDb = new SqlserverHandler(targetConnectString);

        var totalCount = 0L;
        var counter = new Dictionary<object, long>();
        var transferTaskIdentity = sourceConnectString + targetConnectString;

        targetDb.BulkCopyEvent += (sender, args) =>
        {
            lock (counter)
            {
                counter[sender] = args.RowsCopied;
                LoggingHelper.CurrentCounts[transferTaskIdentity] = counter.Values.Sum();
            }
        };

        var tokenSource = new CancellationTokenSource();
        var loggingTask = LoggingHelper.LogForCancel(tokenSource.Token);

        {
            if (!sourceDb.TryConnect(out var e)) throw e;
        }
        {
            if (!targetDb.TryConnect(out var e)) throw e;
        }

        var tasks = new List<Task>();
        foreach (var tblName in sourceDb.GetTableNames().ToArray())
        {
            var table = new DataTable(tblName);
            sourceDb.FillSchema(table);
            targetDb.CreateTable(table);

            var rowCount = sourceDb.GetRowCount(tblName);
            await LoggingHelper.LogTables(tblName, table, ignoreTables, rowCount);

            if (rowCount is 0) continue;
            totalCount += rowCount;
            LoggingHelper.TotalCounts[transferTaskIdentity] = totalCount;

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
                          $"{targetDb.ConnectionStringBuilder.InitialCatalog} for {totalCount} rows");
    }
}