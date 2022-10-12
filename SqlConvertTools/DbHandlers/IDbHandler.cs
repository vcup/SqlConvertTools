using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace SqlConvertTools.DbHandlers;

public interface IDbHandler
{
    public DbConnectionStringBuilder ConnectionStringBuilder { get; }

    public bool TryConnect(bool fallback = true);

    public bool TryConnect([NotNullWhen(false)] out DbException? exception, bool fallback = true);

    public void ChangeDatabase(string dbname);

    public DataSet FillDataset(string tableName, DataSet? dataSet, out int count);

    public DataSet FillSchema(string tableName, DataSet? dataSet);

    public IEnumerable<string> GetDatabases(bool excludeSysDb = true);

    public IEnumerable<string> GetTableNames();

    public void CreateTable(DataTable table);

    public int UpdateDatabaseWith(DataTable table);

    public int GetRowCount(string tableName);
}