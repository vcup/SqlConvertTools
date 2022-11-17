using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using MySqlConnector;
using SqlConvertTools.Extensions;
using SqlConvertTools.Helper;

namespace SqlConvertTools.DbHandlers;

public class MysqlHandler : IDbHandler, IAsyncQueueableDbHandler, IBulkCopyableDbHandler, IDisposable
{
    private MySqlConnection? _connection;
    private readonly MySqlDataAdapter _adapter;


    public MysqlHandler(string address, string password, string user = "root") : this(new MySqlConnectionStringBuilder
    {
        Server = address,
        UserID = user,
        Password = password
    })
    {
    }

    public MysqlHandler(string connectString) : this(new MySqlConnectionStringBuilder(connectString))
    {
    }

    public MysqlHandler(MySqlConnectionStringBuilder connectionStringBuilder)
    {
        _adapter = new MySqlDataAdapter();
        ConnectionStringBuilder = connectionStringBuilder;
    }

    public MySqlConnectionStringBuilder ConnectionStringBuilder { get; }
    DbConnectionStringBuilder IDbHandler.ConnectionStringBuilder => ConnectionStringBuilder;

    public MySqlConnection Connection => _connection ??= new MySqlConnection(ConnectionStringBuilder.ConnectionString);

    public bool TryConnect(bool fallback = true)
    {
        try
        {
            Connection.Open();
        }
        catch
        {
            var dbname = ConnectionStringBuilder.Database;
            if (fallback)
            {
                ConnectionStringBuilder.Database = "mysql";
                _connection!.Dispose();
                _connection = new MySqlConnection(ConnectionStringBuilder.ConnectionString);
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
        catch (MySqlException e)
        {
            exception = e;
            var dbname = ConnectionStringBuilder.Database;
            if (fallback)
            {
                ConnectionStringBuilder.Database = "mysql";
                _connection!.Dispose();
                _connection = new MySqlConnection(ConnectionStringBuilder.ConnectionString);
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
                   .ExecuteReader(@$"Show Databases Where `Database` not regexp 'schema|sys|mysql' and `Database` = '{dbname}'"))
        {
            if (!reader.HasRows)
            {
                reader.Dispose();
                Connection.CreateCommand().ExecuteNonQuery($"Create Database {dbname}");
            }
        }

        Connection.ChangeDatabase(ConnectionStringBuilder.Database = dbname);
    }

    public async Task FillQueueAsync(ConcurrentQueue<DataRow> queue, string tableName, CancellationToken token)
    {
        await using var command = Connection.CreateCommand();
        var table = new DataTable(tableName);
        FillSchema(table);

        await using var reader = command.ExecuteReader($@"SELECT * FROM `{tableName}`");
        var colCount = reader.FieldCount;
        var items = new object?[colCount];

        while (reader.Read() && !token.IsCancellationRequested)
        {
            for (var j = 0; j < colCount;)
            {
                items[j] = reader[j++];
            }

            queue.Enqueue(table.LoadDataRow(items, LoadOption.Upsert));
        }
    }

    public async Task PeekQueueAsync(ConcurrentQueue<DataRow> queue, CancellationToken token,
        CancellationToken forceToken)
    {
        begin:
        if (!queue.TryDequeue(out var row))
        {
            goto begin;
        }

        var table = row.Table;
        var tableName = table.TableName;
        var cmdBuilder = new MySqlCommandBuilder(_adapter);
        _adapter.SelectCommand = Connection.CreateCommand();
        _adapter.SelectCommand.CommandText = $@"SELECT * FROM `{tableName}`";
        cmdBuilder.Dispose();
        cmdBuilder = new MySqlCommandBuilder(_adapter);

        var cmd = cmdBuilder.GetInsertCommand();
        if (DbHelper.TryGetIdentityColumn(table.Columns, out var idCol))
        {
            cmd.CommandText = cmd.CommandText
                .InsertAfter($"`{tableName}` (", $"`{idCol.ColumnName}`, ", StringComparison.OrdinalIgnoreCase)
                .InsertAfter("VALUES (", "@p0, ", StringComparison.OrdinalIgnoreCase);
            cmd.Parameters.Insert(0, new MySqlParameter("@p0", MySqlDbType.Int32, 0, idCol.ColumnName));
        }

        while (!forceToken.IsCancellationRequested)
        {
            if (queue.IsEmpty && token.IsCancellationRequested) break;
            if (!queue.TryDequeue(out row))
            {
                // ReSharper disable once MethodSupportsCancellation
                await Task.Delay(300);
                continue;
            }

            for (var i = 0; i < cmd.Parameters.Count; i++)
            {
                cmd.Parameters[i].Value = row.ItemArray[i];
            }

            // ReSharper disable once MethodSupportsCancellation
            await cmd.ExecuteNonQueryAsync();
        }
    }

    public DataSet FillSchema(string tableName, DataSet? dataSet)
    {
        dataSet ??= new DataSet();
        using var command = Connection.CreateCommand();
        command.CommandText = $@"Select * From `{tableName}`";
        command.CommandType = CommandType.Text;

        _adapter.SelectCommand = command;
        _adapter.FillSchema(dataSet, SchemaType.Mapped, tableName);
        return dataSet;
    }

    public void FillSchema(DataTable table)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = $@"Select * From `{table.TableName}`";
        command.CommandType = CommandType.Text;

        _adapter.SelectCommand = command;
        _adapter.FillSchema(table, SchemaType.Mapped);
    }

    public IEnumerable<string> GetDatabases(bool excludeSysDb = true)
    {
        using var reader = Connection
            .CreateCommand()
            .ExecuteReader($@"Show Databases{(excludeSysDb ? " where `Database` not regexp 'schema|sys|mysql';" : ';')}");
        while (reader.Read())
        {
            yield return (string)reader["Database"];
        }
    }

    public IEnumerable<string> GetTableNames()
    {
        var key = $"Tables_in_{Connection.Database}";
        using var reader = Connection.CreateCommand().ExecuteReader(@$"Show Tables;");
        while (reader.Read())
        {
            yield return (string)reader[key];
        }
    }

    public void CreateTable(DataTable table)
    {
        using var reader = Connection
            .CreateCommand()
            .ExecuteReader($"show tables where `Tables_in_{Connection.Database}` = '{table.TableName}';");
        if (reader.HasRows) return;
        reader.Dispose();
        Connection.CreateCommand().ExecuteNonQuery(SqlHelper.GetCreateTableSqlForMySql(table));
    }

    public int GetRowCount(string tableName)
    {
        return (int)Connection.CreateCommand()
            .ExecuteScalar($"Select COUNT(1) From `{tableName}`");
    }

    public IDbHandler Clone()
    {
        return new MysqlHandler(new MySqlConnectionStringBuilder(ConnectionStringBuilder.ConnectionString));
    }

    public void Dispose()
    {
        _connection?.Dispose();
        _adapter.Dispose();
    }

    public async Task<IDataReader> CreateDataReader(string tableName)
    {
        await using var cloneConnection = Connection.Clone();
        await using var command = cloneConnection.CreateCommand();

        var reader = command.ExecuteReader($@"SELECT * FROM `{tableName}`");
        return reader;
    }

    public async Task BulkCopy(string tableName, IDataReader reader)
    {
        await using var connectionCopy = Connection.CloneWith(ConnectionStringBuilder.ConnectionString);
        var bulkCopy = new MySqlBulkCopy(connectionCopy)
        {
            DestinationTableName = tableName,
            NotifyAfter = 51,
        };
        bulkCopy.MySqlRowsCopied += (sender, args) => BulkCopyEvent.Invoke(sender, args);

        var result = await bulkCopy.WriteToServerAsync(reader);

        var type = typeof(MySqlRowsCopiedEventArgs);
        var args = (MySqlRowsCopiedEventArgs)type.GetConstructor(
            BindingFlags.Instance | BindingFlags.NonPublic,
            Array.Empty<Type>())!
            .Invoke(Array.Empty<object>());
        var property = type.GetProperty("RowsCopied");
        property!.SetValue(args, result.RowsInserted);
        BulkCopyEvent(bulkCopy, args);
        reader.Dispose();
    }

    public event MySqlRowsCopiedEventHandler BulkCopyEvent;
}