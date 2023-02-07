namespace SqlConvertTools.Utils;

public delegate void DbHandleBulkRowsCopiedEventHandler<TArgs>(
    object sender,
    DbHandleBulkRowsCopiedEventArgs<TArgs> args
) where TArgs : EventArgs;
