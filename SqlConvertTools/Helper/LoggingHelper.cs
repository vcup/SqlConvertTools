using System.Collections.Concurrent;
using System.Data;
using System.Text;

namespace SqlConvertTools.Helper;

public static class LoggingHelper
{
    private static readonly object LogLock = new();
    public static ConcurrentDictionary<string, long> TotalCounts { get; set; } = new();
    public static ConcurrentDictionary<string, long> CurrentCounts { get; set; } = new();
    public static long PrevCount { get; set; }

    public static string CurrentTableName { get; set; } = string.Empty;

    public static Task[] InCompleteTasks { get; set; } = Array.Empty<Task>();

    public static async Task LogForCancel(CancellationToken token)
    {
        var str = new StringBuilder(Console.WindowWidth);
        var prevLoggedLength = 0;
        while (!token.IsCancellationRequested)
        {
            str.Append($"{PrevCount:D6}/{TotalCounts.Values.Sum():D6} +{CurrentCounts.Values.Sum() - PrevCount:D5} ~[{CurrentTableName}] |{InCompleteTasks.Length}|");
            if (str.Length < prevLoggedLength) str.Append(' ', prevLoggedLength - str.Length);

            lock (LogLock)
            {
                Console.Write(str.ToString());
                Console.SetCursorPosition(0, Console.CursorTop);
                prevLoggedLength = str.Length;
            }

            PrevCount = CurrentCounts.Values.Sum();
            str.Clear();
            await Task.Delay(300, token);
        }
    }

    public static Task LogTables(string tblName, DataTable table, string[] ignoreTables, int rowCount)
    {
        return Task.Run(() =>
        {
            lock (LogLock)
            {
                Console.WriteLine($"Creating Table: {tblName}{new string(' ', 12)}");
                Console.Write("Columns: ");
                for (var i = 0; i < table.Columns.Count; i++)
                {
                    Console.Write($"[{table.Columns[i].ColumnName}],");
                }

                Console.SetCursorPosition(Console.CursorLeft is 0 ? 0 : Console.CursorLeft - 1, Console.CursorTop);
                Console.WriteLine(' ');

                if (ignoreTables.Any(i => i.Equals(tblName, StringComparison.OrdinalIgnoreCase)))
                {
                    Console.WriteLine($"Ignored table: {tblName}, " +
                                      $"this will skip {rowCount} rows\n");
                    return;
                }

                Console.WriteLine($@"Coping table: {table.TableName}");
                Console.WriteLine($"Rows Count: {rowCount:d4}");

                Console.WriteLine();
            }
        });
    }
}