using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using SqlConvertTools.Utils;

namespace SqlConvertTools.DbHandlers;

public interface IDbHandler
{
    public DbConnectionStringBuilder ConnectionStringBuilder { get; }

    /// <summary>
    /// try connect database
    /// </summary>
    /// <param name="fallback">if take true, will try connect to default database,
    /// of success, create origin target database and check to</param>
    /// <returns>true if success connected, otherwise false</returns>
    public bool TryConnect(bool fallback = true);

    /// <summary>
    /// try connect database
    /// </summary>
    /// <param name="exception">if connect to database has fail, is the exception</param>
    /// <param name="fallback">if take true, will try connect to default database,
    /// of success, create origin target database and check to</param>
    /// <returns>true if success connected, otherwise false</returns>
    public bool TryConnect([NotNullWhen(false)] out DbException? exception, bool fallback = true);

    public void ChangeDatabase(string dbname);

    public DataSet FillSchema(string tableName, DataSet? dataSet);

    public void FillSchema(DataTable table);
    public void FillSchemaWithColumnDefaultValue(DataTable table, DbmsType dbmsType);

    public IEnumerable<string> GetDatabases(bool excludeSysDb = true);

    public IEnumerable<string> GetTableNames();

    public void CreateTable(DataTable table, bool overrideIfExist = false);

    public int GetRowCount(string tableName);

    public IDbHandler Clone();
}