using System.CommandLine;
using SqlConvertTools.Commands;

var rootCommand = new RootCommand();

rootCommand.AddCommand(new SqlServerToSqliteCommand());

await rootCommand.InvokeAsync(args);