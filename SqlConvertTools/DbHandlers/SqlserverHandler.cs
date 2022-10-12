using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Data.SqlClient;
using SqlConvertTools.Extensions;
using SqlConvertTools.Helper;

namespace SqlConvertTools.DbHandlers;

internal class SqlserverHandler : IDbHandler, IDisposable
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

    public DataSet FillDataset(string tableName, DataSet? dataSet, out int count)
    {
        dataSet ??= new DataSet();
        using var command = Connection.CreateCommand();
        command.CommandText = @$"Select * From [{tableName}]";
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
            await using var reader = command.ExecuteReader($@"SELECT * FROM [{tblName}]");
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

    public int UpdateDatabaseWith(DataTable table)
    {
        var tableName = table.TableName;

        _adapter.SelectCommand = Connection.CreateCommand();
        _adapter.SelectCommand.CommandText = $"SELECT * FROM [{tableName}]";
        var cmdBuilder = new SqlCommandBuilder(_adapter);

        var cols = table.Columns;
        var hasIdentity = DbHelper.TryGetIdentityColumn(cols, out var idCol);

        using var cmd = cmdBuilder.GetInsertCommand();
        using var deleteCmd = Connection.CreateCommand();
        SqlParameter delCmdParam = null!;
        if (hasIdentity)
        {
            cmd.CommandText = cmd.CommandText
                .InsertAfter($"[{tableName}] (", $"[{idCol!.ColumnName}], ")
                .InsertAfter("VALUES (", "@p0, ");
            cmd.Parameters.Insert(0, new SqlParameter("@p0", SqlDbType.Int, 0, idCol.ColumnName));
            deleteCmd.CommandText = $"DELETE FROM [{tableName}] WHERE [{idCol.ColumnName}] = @p0";
            delCmdParam = deleteCmd.Parameters.Add("@p0", SqlDbType.Int, 0, idCol.ColumnName)!;
        }

        var rows = table.Rows;
        if (hasIdentity) Connection.CreateCommand().ExecuteNonQuery($"SET IDENTITY_INSERT [{tableName}] ON");
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

        if (hasIdentity) Connection.CreateCommand().ExecuteNonQuery($"SET IDENTITY_INSERT [{tableName}] OFF");
        return rows.Count;
    }

    public int UpdateDatabaseWith(DataSet dataSet, string? targetDb = null)
    {
        if (targetDb is not null)
        {
            ChangeDatabase(targetDb);
        }

        var rowCount = 0;
        for (var i = 0; i < dataSet.Tables.Count; i++)
        {
            var table = dataSet.Tables[i];
            if (targetDb is not null) // than try create table 
            {
                CreateTable(table);
            }

            rowCount += UpdateDatabaseWith(table);
        }

        return rowCount;
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