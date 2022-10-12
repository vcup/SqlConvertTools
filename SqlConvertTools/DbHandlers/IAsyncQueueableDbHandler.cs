using System.Collections.Concurrent;
using System.Data;

namespace SqlConvertTools.DbHandlers;

public interface IAsyncQueueableDbHandler
{
    public Task FillQueueAsync(ConcurrentQueue<DataRow> queue, IEnumerable<string> tables, CancellationToken token);

    public Task PeekQueueAsync(ConcurrentQueue<DataRow> queue, CancellationToken token, CancellationToken forceToken);

    public event Func<DataTable, bool> BeforeFillNewTable;
}