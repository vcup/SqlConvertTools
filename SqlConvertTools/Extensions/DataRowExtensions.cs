using System.Data;
using System.Reflection;

namespace SqlConvertTools.Extensions;

public static class DataRowExtensions
{
    private static readonly FieldInfo OldRecord;

    private static readonly FieldInfo NewRecord;

    static DataRowExtensions()
    {
        var type = typeof(DataRow);
        OldRecord = type.GetField("_oldRecord", BindingFlags.Instance | BindingFlags.NonPublic)!;
        // _newRecord meaning how many row has add/delete/modify for row.Table
        NewRecord = type.GetField("_newRecord", BindingFlags.Instance | BindingFlags.NonPublic)!;
    }


    public static void SetState(this DataRow row, DataRowState state)
    {
        switch (state)
        {
            case DataRowState.Detached:
                OldRecord.SetValue(row, -1);
                NewRecord.SetValue(row, -1);
                break;
            case DataRowState.Unchanged:
                OldRecord.SetValue(row, 0);
                NewRecord.SetValue(row, 0);
                break;
            case DataRowState.Added:
                OldRecord.SetValue(row, -1);
                if (NewRecord.GetValue(row) is -1)
                {
                    NewRecord.SetValue(row, 0);
                }
                break;
            case DataRowState.Deleted:
                OldRecord.SetValue(row, 0);
                NewRecord.SetValue(row, -1);
                break;
            case DataRowState.Modified:
                OldRecord.SetValue(row, 0);
                NewRecord.SetValue(row, 1);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(state), state, null);
        }
    }
}