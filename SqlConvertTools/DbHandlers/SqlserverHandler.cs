using System.Data;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Data.SqlClient;
using SqlConvertTools.Extensions;

namespace SqlConvertTools.DbHandlers;

internal class SqlserverHandler : IDisposable
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

    private SqlConnection Connection => _connection ??= new SqlConnection(ConnectionStringBuilder.ConnectionString);

    public bool TryConnect(bool fallback = true)
    {
        try
        {
            Connection.Open();
        }
        catch
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
        ChangeDatabase(dbname);
        return flag;
    }

    public bool TryConnect([NotNullWhen(false)] out SqlException? exception, bool fallback = true)
    {
        try
        {
            Connection.Open();
        }
        catch (SqlException e)
        {
            exception = e;
            
            if (fallback)
            {
                ConnectionStringBuilder.InitialCatalog = "master";
                _connection!.Dispose();
                _connection = new SqlConnection(ConnectionStringBuilder.ConnectionString);
            }
            return fallback && TryConnect(out exception, false);
        }

        exception = null;
        return true;
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

    public DataSet FillDatasetWithoutData(string tableName, DataSet? dataSet)
    {
        dataSet ??= new DataSet();
        using var command = Connection.CreateCommand();
        command.CommandText = $@"Select * From [{tableName}]";
        command.CommandType = CommandType.Text;

        _adapter.SelectCommand = command;
        _adapter.FillSchema(dataSet, SchemaType.Mapped, tableName);
        return dataSet;
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
            Connection.CreateCommand().ExecuteNonQuery(SqlHelper.GetCreateTableSql(table));
        }
    }

    public bool TryGetIdentityColumn(DataColumnCollection cols, [NotNullWhen(true)] out DataColumn? idCol)
    {
        idCol = null;
        var flag = false;
        for (var j = 0; j < cols.Count; j++)
        {
            if (!flag && (flag |= cols[j].AutoIncrement))
            {
                idCol = cols[j];
            }
        }

        return flag;
    }

    public int UpdateDatabaseWith(DataTable table)
    {
        var tableName = table.TableName;

        _adapter.SelectCommand = Connection.CreateCommand();
        _adapter.SelectCommand.CommandText = $"SELECT * FROM [{tableName}]";
        var cmdBuilder = new SqlCommandBuilder(_adapter);

        var cols = table.Columns;
        var hasIdentity = TryGetIdentityColumn(cols, out var idCol);

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