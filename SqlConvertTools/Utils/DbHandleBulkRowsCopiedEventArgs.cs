namespace SqlConvertTools.Utils;

public class DbHandleBulkRowsCopiedEventArgs<TArgs> : EventArgs
 where TArgs : EventArgs
{
    public TArgs? EventArguments { get; internal set; }

    public string TableName { get; init; } = string.Empty;
}