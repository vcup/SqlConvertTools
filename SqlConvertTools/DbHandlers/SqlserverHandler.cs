using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Data.SqlClient;
using SqlConvertTools.Extensions;
using SqlConvertTools.Helper;

namespace SqlConvertTools.DbHandlers;

internal class SqlserverHandler : IDbHandler, IAsyncQueueableDbHandler, IBulkCopyableDbHandler, IDisposable
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

    public SqlConnectionStringBuilder ConnectionStringBuilder { get; }
    DbConnectionStringBuilder IDbHandler.ConnectionStringBuilder => ConnectionStringBuilder;

    private SqlConnection Connection => _connection ??= new SqlConnection(ConnectionStringBuilder.ConnectionString);

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
        using (var reader = Connection.CreateCommand()
                   .ExecuteReader(@$"Select name From sys.databases Where database_id > 4 And name = '{dbname}'"))
        {
            if (dbname is not "master" && !reader.HasRows)
            {
                reader.Dispose();
                Connection.CreateCommand().ExecuteNonQuery($"Create Database {dbname}");
            }
        }

        Connection.ChangeDatabase(dbname);
    }

    public async Task FillQueueAsync(ConcurrentQueue<DataRow> queue, string tableName, CancellationToken token)
    {
        await using var command = Connection.CreateCommand();
        var table = new DataTable(tableName);
        FillSchema(table);

        await using var reader = command.ExecuteReader($@"SELECT * FROM [{tableName}]");
        var colCount = reader.FieldCount;
        var items = new object?[colCount];

        while (await reader.ReadAsync(token) && !token.IsCancellationRequested)
        {
            for (var j = 0; j < colCount;)
            {
                items[j] = reader[j++];
            }

            queue.Enqueue(table.LoadDataRow(items, LoadOption.Upsert));
        }
    }

    public Task PeekQueueAsync(ConcurrentQueue<DataRow> queue, CancellationToken token, CancellationToken forceToken)
    {
        throw new NotImplementedException("origin impl is not tested");
    }

    public DataSet FillSchema(string tableName, DataSet? dataSet)
    {
        dataSet ??= new DataSet();
        using var command = Connection.CreateCommand();
        command.CommandText = $@"Select * From [{tableName}]";
        command.CommandType = CommandType.Text;

        _adapter.SelectCommand = command;
        _adapter.FillSchema(dataSet, SchemaType.Mapped, tableName);
        return dataSet;
    }

    public void FillSchema(DataTable table)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = $@"Select * From [{table.TableName}]";
        command.CommandType = CommandType.Text;

        _adapter.SelectCommand = command;
        _adapter.FillSchema(table, SchemaType.Mapped);
    }

    public IEnumerable<string> GetDatabases(bool excludeSysDb = true)
    {
        using var reader = Connection
            .CreateCommand()
            .ExecuteReader($@"Select name From sys.databases {(excludeSysDb ? "Where database_id > 4" : "")}");
        while (reader.Read())
        {
            yield return (string)reader["name"];
        }
    }

    public IEnumerable<string> GetTableNames()
    {
        using var reader = Connection.CreateCommand().ExecuteReader(@$"SELECT name FROM sys.tables;");
        while (reader.Read())
        {
            yield return (string)reader["name"];
        }
    }

    public void CreateTable(DataTable table)
    {
        var exist = (int)Connection
            .CreateCommand()
            .ExecuteScalar(
                "IF EXISTS (" +
                "SELECT 1 FROM INFORMATION_SCHEMA.TABLES " +
                "WHERE TABLE_TYPE='BASE TABLE' AND " +
                $"TABLE_NAME='{table.TableName}'" +
                ") SELECT 1 ELSE SELECT 0;");
        if (exist == 0)
        {
            Connection.CreateCommand().ExecuteNonQuery(SqlHelper.GetCreateTableSqlForSqlserver(table));
        }
    }

    public int GetRowCount(string tableName)
    {
        return (int)Connection.CreateCommand()
            .ExecuteScalar($"Select COUNT(1) From [{tableName}]");
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
        await using var newConnection = new SqlConnection(ConnectionStringBuilder.ConnectionString);
        await newConnection.OpenAsync();
        await using var command = newConnection.CreateCommand();

        var reader = command.ExecuteReader($@"SELECT * FROM [{tableName}]");
        return reader;
    }

    public Task BulkCopy(string tableName, IDataReader reader)
    {
        throw new NotImplementedException();
    }
}