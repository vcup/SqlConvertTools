using System.Data;
using SqlConvertTools.DbHandlers;

namespace SqlConvertTools.Extensions;

internal static class SqlserverHandlerExtensions
{
    public static DataSet FillDataset(this SqlserverHandler handler, DataSet dataSet, out int rowCount)
    {
        rowCount = 0;
        foreach (var table in handler.GetTableNames().ToArray())
        {
            handler.FillDataset(table, dataSet, out var c);
            rowCount += c;
        }

        return dataSet;
    }

    public static DataSet FillDatasetWithoutData(this SqlserverHandler handler, DataSet dataSet)
    {
        var tables = handler.GetTableNames().ToArray();
        foreach (var table in tables)
        {
            handler.FillSchema(table, dataSet);
        }

        return dataSet;
    }
}