using System.CommandLine;
using SqlConvertTools.Commands;

var rootCommand = new RootCommand();

rootCommand.AddCommand(new SqlServerToSqliteCommand());
rootCommand.AddCommand(new SqlServerTransferCommand());
rootCommand.AddCommand(new SqliteToSqlserverCommand());
rootCommand.AddCommand(new SqlServerToMySqlCommand());

return await rootCommand.InvokeAsync(args);