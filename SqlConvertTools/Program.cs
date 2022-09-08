using System.CommandLine;
using SqlConvertTools.Commands;

var rootCommand = new RootCommand();

rootCommand.AddCommand(new SqlServerToSqliteCommand());
rootCommand.AddCommand(new SqlServerTransferCommand());

return await rootCommand.InvokeAsync(args);