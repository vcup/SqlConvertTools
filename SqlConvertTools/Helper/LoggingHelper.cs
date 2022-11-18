using System.Data;

namespace SqlConvertTools.Helper;

public static class LoggingHelper
{
    private static readonly object LogLock = new();
    public static long TotalCount;
    public static long CurrentCount;
    public static long PrevCount;

    public static async Task LogForCancel(CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            lock (LogLock)
            {
                Console.WriteLine($"{PrevCount:D6}/{TotalCount:D6} +{CurrentCount - PrevCount:D4}");
                Console.SetCursorPosition(0, Console.CursorTop - 1);
            }

            PrevCount = CurrentCount;
            await Task.Delay(300, token);
        }
    }

    public static Task LogTables(string tblName, DataTable table, string[] ignoreTables, int rowCount)
    {
        return Task.Run(() =>
        {
            lock (LogLock)
            {
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

                if (ignoreTables.Contains(tblName.ToLower()))
                {
                    Console.WriteLine($"Ignored table: {tblName}, " +
                                      $"this will skip {rowCount} row\n");
                    return;
                }

                Console.WriteLine($@"Coping table: {table.TableName}");
                Console.WriteLine($"Rows Count: {rowCount:d4}");

                Console.WriteLine();
            }
        });
    }
}