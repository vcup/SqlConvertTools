using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using Microsoft.Data.SqlClient;
using SqlConvertTools.Extensions;
using SqlConvertTools.Helper;

namespace SqlConvertTools.DbHandlers;

internal class SqlserverHandler : IDbHandler, IBulkCopyableDbHandler, IDisposable
{
    private SqlConnection? _connection;
    private readonly SqlDataAdapter _adapter;

    public SqlserverHandler(string address, string password, string user = "sa") : this(new SqlConnectionStringBuilder
    {
        DataSource = address,
        UserID = user,
        Password = password
    })
    {
    }

    public SqlserverHandler(string connectString) : this(new SqlConnectionStringBuilder(connectString))
    {
    }

    public SqlserverHandler(SqlConnectionStringBuilder connectionStringBuilder)
    {
        _adapter = new SqlDataAdapter
        {
            MissingSchemaAction = MissingSchemaAction.AddWithKey,
        };
        ConnectionStringBuilder = connectionStringBuilder;
    }

    public Dictionary<string, string> SchemaMap { get; } = new();

    public SqlConnectionStringBuilder ConnectionStringBuilder { get; }
    DbConnectionStringBuilder IDbHandler.ConnectionStringBuilder => ConnectionStringBuilder;

    private SqlConnection Connection => _connection ??= new SqlConnection(ConnectionStringBuilder.ConnectionString);

    /// <summary>
    /// try to connect to database, default database using 'master'
    /// </summary>
    /// <inheritdoc/>
    public bool TryConnect(bool fallback = true)
    {
        try
        {
            Connection.Open();
        }
        catch (SqlException)
        {
            var dbname = ConnectionStringBuilder.InitialCatalog;
            if (fallback)
            {
                ConnectionStringBuilder.InitialCatalog = "master";
                _connection!.Dispose();
                _connection = new SqlConnection(ConnectionStringBuilder.ConnectionString);
            }

            return fallback && dbname is not null && TryConnect(dbname);
        }

        return true;
    }

    private bool TryConnect(string dbname)
    {
        var flag = TryConnect(false);
        if (flag) ChangeDatabase(dbname);
        return flag;
    }

    /// <summary>
    /// try to connect to database, default database using 'master' for <paramref name="fallback"/>
    /// </summary>
    /// <inheritdoc/>
    public bool TryConnect([NotNullWhen(false)] out DbException? exception, bool fallback = true)
    {
        try
        {
            Connection.Open();
        }
        catch (SqlException e)
        {
            exception = e;
            var dbname = ConnectionStringBuilder.InitialCatalog;

            if (fallback)
            {
                ConnectionStringBuilder.InitialCatalog = "master";
                _connection!.Dispose();
                _connection = new SqlConnection(ConnectionStringBuilder.ConnectionString);
            }

            return fallback && dbname is not null && TryConnect(out exception, dbname);
        }

        exception = null;
        return true;
    }

    private bool TryConnect([NotNullWhen(false)] out DbException? exception, string dbname)
    {
        var flag = TryConnect(out exception, false);
        if (flag) ChangeDatabase(dbname);
        return flag;
    }

    public void ChangeDatabase(string dbname)
    {
        var command = Connection.CreateCommand();
        command.CommandTimeout = ParsedOptions.SourceCommandTimeout;
        using (var reader = command
                   .ExecuteReader(@$"Select name From sys.databases Where database_id > 4 And name = '{dbname}'"))
        {
            if (dbname is not "master" && !reader.HasRows)
            {
                reader.Dispose();
                command = Connection.CreateCommand();
                command.CommandTimeout = ParsedOptions.SourceCommandTimeout;
                command.ExecuteNonQuery($"Create Database {dbname}");
            }
        }

        Connection.ChangeDatabase(ConnectionStringBuilder.InitialCatalog = dbname);
    }

    public void FillSchemaMap(params string[] tables)
    {
        var tblArrInSql = new StringBuilder();
        foreach (var table in tables)
        {
            tblArrInSql.Append($"'{table}',");
        }

        tblArrInSql.Remove(tblArrInSql.Length - 1, 1);

        using var command = Connection.CreateCommand();
        command.CommandTimeout = ParsedOptions.SourceCommandTimeout;
        command.CommandText = $"SELECT TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME in ({tblArrInSql})";
        command.CommandType = CommandType.Text;

        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            SchemaMap[(reader["TABLE_NAME"] as string)!] = (reader["TABLE_SCHEMA"] as string)!;
        }
    }

    public DataSet FillSchema(string tableName, DataSet? dataSet)
    {
        if (!SchemaMap.TryGetValue(tableName, out var schema)) schema = "dbo";

        dataSet ??= new DataSet();
        using var command = Connection.CreateCommand();
        command.CommandTimeout = ParsedOptions.SourceCommandTimeout;
        command.CommandText = $@"Select * From [{schema}].[{tableName}]";
        command.CommandType = CommandType.Text;

        _adapter.SelectCommand = command;
        _adapter.FillSchema(dataSet, SchemaType.Mapped, tableName);
        return dataSet;
    }

    public void FillSchema(DataTable table)
    {
        if (!SchemaMap.TryGetValue(table.TableName, out var schema)) schema = "dbo";

        using var command = Connection.CreateCommand();
        command.CommandTimeout = ParsedOptions.SourceCommandTimeout;
        command.CommandText = $@"Select * From [{schema}].[{table.TableName}]";
        command.CommandType = CommandType.Text;

        _adapter.SelectCommand = command;
        _adapter.FillSchema(table, SchemaType.Mapped);
    }

    public IEnumerable<string> GetDatabases(bool excludeSysDb = true)
    {
        var command = Connection.CreateCommand();
        command.CommandTimeout = ParsedOptions.SourceCommandTimeout;
        using var reader = command
            .ExecuteReader($@"Select name From sys.databases {(excludeSysDb ? "Where database_id > 4" : "")}");
        while (reader.Read())
        {
            yield return (string)reader["name"];
        }
    }

    public IEnumerable<string> GetTableNames()
    {
        var command = Connection.CreateCommand();
        command.CommandTimeout = ParsedOptions.SourceCommandTimeout;
        using var reader = command.ExecuteReader(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES " +
            "WHERE TABLE_NAME NOT IN (SELECT name FROM sys.objects WHERE is_ms_shipped = 1) " +
            "AND TABLE_TYPE = 'BASE TABLE';"
        );
        while (reader.Read())
        {
            yield return (string)reader["TABLE_NAME"];
        }
    }

    public void CreateTable(DataTable table, bool overrideIfExist = false)
    {
        var command = Connection.CreateCommand();
        command.CommandTimeout = ParsedOptions.SourceCommandTimeout;
        var exist = (int)command
            .ExecuteScalar(
                "IF EXISTS (" +
                "SELECT 1 FROM INFORMATION_SCHEMA.TABLES " +
                "WHERE TABLE_TYPE='BASE TABLE' AND " +
                $"TABLE_NAME='{table.TableName}'" +
                ") SELECT 1 ELSE SELECT 0;");
        if (exist == 0)
        {
            command = Connection.CreateCommand();
            command.CommandTimeout = ParsedOptions.SourceCommandTimeout;
            command.ExecuteNonQuery(SqlHelper.GetCreateTableSqlForSqlserver(table));
        }
    }

    public int GetRowCount(string tableName)
    {
        if (!SchemaMap.TryGetValue(tableName, out var schema)) schema = "dbo";
        var command = Connection.CreateCommand();
        command.CommandTimeout = ParsedOptions.SourceCommandTimeout;
        return (int)command.ExecuteScalar($"Select COUNT(1) From [{schema}].[{tableName}]");
    }

    public IDbHandler Clone()
    {
        return new SqlserverHandler(ConnectionStringBuilder.ConnectionString);
    }

    public void Dispose()
    {
        _connection?.Dispose();
        _adapter.Dispose();
    }

    public async Task<IDataReader> CreateDataReader(string tableName)
    {
        if (!SchemaMap.TryGetValue(tableName, out var schema)) schema = "dbo";

        var newConnection = new SqlConnection(ConnectionStringBuilder.ConnectionString);
        await newConnection.OpenAsync();
        await using var command = newConnection.CreateCommand();
        command.CommandTimeout = ParsedOptions.SourceCommandTimeout;

        var reader = command.ExecuteReader($@"SELECT * FROM [{schema}].[{tableName}]");
        return reader;
    }

    public async Task BulkCopy(string tableName, IDataReader reader)
    {
        await using var connection = new SqlConnection(ConnectionStringBuilder.ConnectionString);
        while (true)
        {
            try
            {
                // if target Database is just create in a moment, it may report sql exception
                await connection.OpenAsync();
            }
            catch (SqlException)
            {
                await Task.Delay(300);
                continue;
            }

            break;
        }

        var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, null)
        {
            DestinationTableName = tableName,
            NotifyAfter = 51,
        };
        bulkCopy.SqlRowsCopied += BulkCopyEvent;

        await bulkCopy.WriteToServerAsync(reader);

        var args = new SqlRowsCopiedEventArgs(bulkCopy.RowsCopied);
        BulkCopyEvent?.Invoke(bulkCopy, args);
        reader.Dispose(true);
    }

    public event SqlRowsCopiedEventHandler? BulkCopyEvent;
}