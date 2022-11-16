using System.Collections.Concurrent;
using System.Data;

namespace SqlConvertTools.DbHandlers;

public interface IAsyncQueueableDbHandler
{
    public Task FillQueueAsync(ConcurrentQueue<DataRow> queue, string tableName, CancellationToken token);

    public Task PeekQueueAsync(ConcurrentQueue<DataRow> queue, CancellationToken token, CancellationToken forceToken);
}