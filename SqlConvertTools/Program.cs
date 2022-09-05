using System.CommandLine;
using SqlConvertTools.Commands;

var rootCommand = new RootCommand();

rootCommand.AddCommand(new SqlServerToSqliteCommand());

return await rootCommand.InvokeAsync(args);