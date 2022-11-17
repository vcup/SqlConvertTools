using System.Data;

namespace SqlConvertTools.DbHandlers;

public interface IBulkCopyableDbHandler
{
    public Task<IDataReader> CreateDataReader(string tableName);

    public Task BulkCopy(string tableName, IDataReader reader);
}