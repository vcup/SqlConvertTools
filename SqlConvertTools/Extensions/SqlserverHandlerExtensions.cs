using System.Data;
using SqlConvertTools.DbHandlers;

namespace SqlConvertTools.Extensions;

internal static class SqlserverHandlerExtensions
{
    public static int FillDatasetWithDatabase(this SqlserverHandler handler, DataSet dataSet, out int rowCount)
    {
        var tables = handler.GetTableNames().ToArray();
        rowCount = 0;
        foreach (var table in tables)
        {
            handler.FillDatasetWithTable(table, dataSet, out var c);
            rowCount += c;
        }

        return tables.Length;
    }
}