using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using MySqlConnector;
using SqlConvertTools.Extensions;
using SqlConvertTools.Helper;
using SqlConvertTools.Utils;

namespace SqlConvertTools.DbHandlers;

public class MysqlHandler : IDbHandler, IBulkCopyableDbHandler, IDisposable
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

    /// <summary>
    /// try to connect to database, default database using 'mysql' for <paramref name="fallback"/>
    /// </summary>
    /// <inheritdoc/>
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

    /// <summary>
    /// try to connect to database, default database using 'mysql' for <paramref name="fallback"/>
    /// </summary>
    /// <inheritdoc/>
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
        var command = Connection.CreateCommand();
        command.CommandTimeout = ParsedOptions.TargetCommandTimeout;
        using (var reader = command
                   .ExecuteReader(@$"Show Databases Where `Database` not regexp 'schema|sys|mysql' and `Database` = '{dbname}'"))
        {
            if (!reader.HasRows)
            {
                reader.Dispose();
                command = Connection.CreateCommand();
                command.CommandTimeout = ParsedOptions.TargetCommandTimeout;
                command.ExecuteNonQuery($"Create Database {dbname}");
            }
        }

        Connection.ChangeDatabase(ConnectionStringBuilder.Database = dbname);
    }

    public DataSet FillSchema(string tableName, DataSet? dataSet)
    {
        dataSet ??= new DataSet();
        using var command = Connection.CreateCommand();
        command.CommandTimeout = ParsedOptions.TargetCommandTimeout;
        command.CommandText = $@"Select * From `{tableName}`";
        command.CommandType = CommandType.Text;

        _adapter.SelectCommand = command;
        _adapter.FillSchema(dataSet, SchemaType.Mapped, tableName);
        return dataSet;
    }

    public void FillSchema(DataTable table)
    {
        using var command = Connection.CreateCommand();
        command.CommandTimeout = ParsedOptions.TargetCommandTimeout;
        command.CommandText = $@"Select * From `{table.TableName}`";
        command.CommandType = CommandType.Text;

        _adapter.SelectCommand = command;
        _adapter.FillSchema(table, SchemaType.Mapped);
    }

    public void FillSchemaWithColumnDefaultValue(DataTable table, DbmsType dbmsType) =>
        throw new NotImplementedException();

    public IEnumerable<string> GetDatabases(bool excludeSysDb = true)
    {
        var command = Connection.CreateCommand();
        command.CommandTimeout = ParsedOptions.TargetCommandTimeout;
        using var reader = command
            .ExecuteReader($@"Show Databases{(excludeSysDb ? " where `Database` not regexp 'schema|sys|mysql';" : ';')}");
        while (reader.Read())
        {
            yield return (string)reader["Database"];
        }
    }

    public IEnumerable<string> GetTableNames()
    {
        var key = $"Tables_in_{Connection.Database}";
        var command = Connection.CreateCommand();
        command.CommandTimeout = ParsedOptions.TargetCommandTimeout;
        using var reader = command.ExecuteReader(@$"Show Tables;");
        while (reader.Read())
        {
            yield return (string)reader[key];
        }
    }

    public void CreateTable(DataTable table, bool overrideIfExist = false)
    {
        var command = Connection.CreateCommand();
        command.CommandTimeout = ParsedOptions.TargetCommandTimeout;
        if (overrideIfExist)
        {
            command.ExecuteNonQuery($"DROP Table IF EXISTS `{table.TableName}`");
        }
        else
        {
            using var reader = command
                .ExecuteReader($"show tables where `Tables_in_{Connection.Database}` = '{table.TableName}';");
            if (reader.HasRows) return;
            reader.Dispose();
        }

        command = Connection.CreateCommand();
        command.CommandTimeout = ParsedOptions.TargetCommandTimeout;
        command.ExecuteNonQuery(SqlHelper.GetCreateTableSqlForMySql(table));
    }

    public int GetRowCount(string tableName)
    {
        var command = Connection.CreateCommand();
        command.CommandTimeout = ParsedOptions.TargetCommandTimeout;
        return (int)command
            .ExecuteScalar($"Select COUNT(1) From `{tableName}`")!;
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
        command.CommandTimeout = ParsedOptions.TargetCommandTimeout;

        var reader = command.ExecuteReader($@"SELECT * FROM `{tableName}`");
        return reader;
    }

    public async Task BulkCopy(string tableName, IDataReader reader)
    {
        await using var connectionCopy = Connection.CloneWith(ConnectionStringBuilder.ConnectionString);
        var bulkCopy = new MySqlBulkCopy(connectionCopy)
        {
            DestinationTableName = '`' + tableName + '`',
            NotifyAfter = 51,
        };
        var eventArgs = new DbHandleBulkRowsCopiedEventArgs<MySqlRowsCopiedEventArgs>()
        {
            TableName = tableName
        };
        bulkCopy.MySqlRowsCopied += (sender, args) =>
        {
            eventArgs.EventArguments = args;
            BulkCopyEvent?.Invoke(sender, eventArgs);
        };

        var result = await bulkCopy.WriteToServerAsync(reader);

        // make sure all of copied rows can raise after bulk copy task end
        var type = typeof(MySqlRowsCopiedEventArgs);
        var args = (MySqlRowsCopiedEventArgs)type.GetConstructor(
                BindingFlags.Instance | BindingFlags.NonPublic,
                Array.Empty<Type>())!
            .Invoke(Array.Empty<object>());
        var property = type.GetProperty("RowsCopied");
        property!.SetValue(args, result.RowsInserted);

        eventArgs.EventArguments = args;
        BulkCopyEvent?.Invoke(bulkCopy, eventArgs);
        reader.Dispose(true);
    }

    public event DbHandleBulkRowsCopiedEventHandler<MySqlRowsCopiedEventArgs>? BulkCopyEvent;
}