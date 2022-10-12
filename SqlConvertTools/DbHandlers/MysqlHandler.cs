using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using MySql.Data.MySqlClient;
using SqlConvertTools.Extensions;
using SqlConvertTools.Helper;

namespace SqlConvertTools.DbHandlers;

public class MysqlHandler : IDbHandler, IDisposable
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

    public DataSet FillDataset(string tableName, DataSet? dataSet, out int count)
    {
        dataSet ??= new DataSet();
        using var command = Connection.CreateCommand();
        command.CommandText = @$"Select * From `{tableName}`";
        command.CommandType = CommandType.Text;

        _adapter.SelectCommand = command;
        count = _adapter.Fill(dataSet, tableName);

        return dataSet;
    }

    public async Task FillQueueAsync(ConcurrentQueue<DataRow> queue, IEnumerable<string> tables, CancellationToken token)
    {
        await using var command = Connection.CreateCommand();
        foreach (var tblName in tables)
        {
            var table = new DataTable(tblName);
            FillSchema(table);
            await using var reader = command.ExecuteReader($@"SELECT * FROM `{tblName}`");
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
    }

    public Task PeekQueueAsync(ConcurrentQueue<DataRow> queue, CancellationToken token, CancellationToken forceToken)
    {
        var tableName = string.Empty;
        using var cmdBuilder = new MySqlCommandBuilder();
        MySqlCommand cmd = null!;
        var delCmd = Connection.CreateCommand();
        MySqlParameter delParameter = null!;

        var hasIdentity = false;
        DataColumn? idCol = null;
        while (forceToken.IsCancellationRequested)
        {
            if (queue.IsEmpty && token.IsCancellationRequested) return Task.CompletedTask;
            if (!queue.TryDequeue(out var row)) continue;
            // init think for new Table
            if (tableName != row.Table.TableName)
            {
                var table = row.Table;
                tableName = table.TableName;
                cmdBuilder.DataAdapter.SelectCommand = Connection.CreateCommand();
                cmdBuilder.DataAdapter.SelectCommand.CommandText = $@"SELECT * FROM `{tableName}`";
                cmd = cmdBuilder.GetInsertCommand();
                // ReSharper disable once AssignmentInConditionalExpression
                if (hasIdentity = DbHelper.TryGetIdentityColumn(table.Columns, out idCol))
                {
                    cmd.CommandText = cmd.CommandText
                        .InsertAfter($"`{tableName}` (", $"`{idCol!.ColumnName}`, ", StringComparison.OrdinalIgnoreCase)
                        .InsertAfter("VALUES (", "@p0, ", StringComparison.OrdinalIgnoreCase);
                    cmd.Parameters.Insert(0, new MySqlParameter("@p0", MySqlDbType.Int32, 0, idCol.ColumnName));
                    delCmd.CommandText = $@"DELETE FROM `{tableName}` WHERE `{idCol.ColumnName}` = @p0";
                    delParameter = delCmd.Parameters.Add("@p0", MySqlDbType.Int32, 0, idCol.ColumnName);
                }
            }

            for (var i = 0; i < cmd.Parameters.Count; i++)
            {
                cmd.Parameters[i].Value = row.ItemArray[i];
            }

            if (hasIdentity)
            {
                delParameter.Value = row[idCol!];
                delCmd.ExecuteNonQuery();
            }

            cmd.ExecuteNonQuery();
        }
        
        return forceToken.IsCancellationRequested ? Task.FromCanceled(forceToken) : Task.CompletedTask;
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
            .ExecuteReader($@"Show Databases {(excludeSysDb ? "where `Database` not regexp 'schema|sys|mysql';" : "")}");
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

    public int UpdateDatabaseWith(DataTable table)
    {
        var tableName = table.TableName;

        _adapter.SelectCommand = Connection.CreateCommand();
        _adapter.SelectCommand.CommandText = $"SELECT * FROM `{tableName}`";
        var cmdBuilder = new MySqlCommandBuilder(_adapter);

        var cols = table.Columns;
        var hasIdentity = DbHelper.TryGetIdentityColumn(cols, out var idCol);

        using var cmd = cmdBuilder.GetInsertCommand();
        using var deleteCmd = Connection.CreateCommand();
        MySqlParameter delCmdParam = null!;
        if (hasIdentity)
        {
            cmd.CommandText = cmd.CommandText
                .InsertAfter($"`{tableName}` (", $"`{idCol!.ColumnName}`, ", StringComparison.OrdinalIgnoreCase)
                .InsertAfter("VALUES (", "@p0, ", StringComparison.OrdinalIgnoreCase);
            cmd.Parameters.Insert(0, new MySqlParameter("@p0", MySqlDbType.Int32, 0, idCol.ColumnName));
            deleteCmd.CommandText = $"DELETE FROM `{tableName}` WHERE `{idCol.ColumnName}` = @p0";
            delCmdParam = deleteCmd.Parameters.Add("@p0", MySqlDbType.Int32, 0, idCol.ColumnName)!;
        }

        var rows = table.Rows;
        for (var j = 0; j < rows.Count; j++)
        {
            for (var k = 0; k < cmd.Parameters.Count; k++)
            {
                cmd.Parameters[k].Value = rows[j].ItemArray[k];
            }

            if (hasIdentity)
            {
                delCmdParam.Value = rows[j][idCol!];
                deleteCmd.ExecuteNonQuery();
            }

            cmd.ExecuteNonQuery();
        }

        return rows.Count;
    }

    public int GetRowCount(string tableName)
    {
        
        return (int)Connection.CreateCommand()
            .ExecuteScalar($"Select COUNT(1) From [{tableName}]");
    }

    public void Dispose()
    {
        _connection?.Dispose();
        _adapter.Dispose();
    }
}