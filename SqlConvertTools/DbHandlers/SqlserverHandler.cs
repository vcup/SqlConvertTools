using System.Data;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Data.SqlClient;
using SqlConvertTools.Extensions;

namespace SqlConvertTools.DbHandlers;

internal class SqlserverHandler : IDisposable
{
    private SqlConnection? _connection;
    private readonly SqlDataAdapter _adapter;
    private SqlCommandBuilder _commandBuilder;

    public SqlserverHandler(string address, string password, string user = "sa")
    {
        _adapter = new SqlDataAdapter
        {
            MissingSchemaAction = MissingSchemaAction.AddWithKey,
        };
        _commandBuilder = new SqlCommandBuilder(_adapter);
        ConnectionStringBuilder = new SqlConnectionStringBuilder
        {
            DataSource = address,
            UserID = user,
            Password = password,
        };
    }

    public SqlConnectionStringBuilder ConnectionStringBuilder { get; }

    private SqlConnection Connection => _connection ??= new SqlConnection(ConnectionStringBuilder.ConnectionString);

    public bool TryConnect([NotNullWhen(false)] out SqlException? exception)
    {
        try
        {
            Connection.Open();
        }
        catch (SqlException e)
        {
            exception = e;
            return false;
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

    public DataSet FillDatasetWithTable(string tableName, DataSet? dataSet, out int count)
    {
        dataSet ??= new DataSet();
        using var command = Connection.CreateCommand();
        command.CommandText = @$"Select * From {tableName}";
        command.CommandType = CommandType.Text;

        _adapter.SelectCommand = command;
        count = _adapter.Fill(dataSet, tableName);

        return dataSet;
    }

    public int FillDatasetWithDatabase(DataSet dataSet, out int rowCount)
    {
        var tables = new List<string>();
        using (var reader = Connection.CreateCommand().ExecuteReader(@$"SELECT name FROM sys.tables;"))
        {
            while (reader.Read())
            {
                tables.Add((string)reader["name"]);
            }
        }

        rowCount = 0;
        foreach (var table in tables)
        {
            FillDatasetWithTable(table, dataSet, out var c);
            rowCount += c;
        }

        return tables.Count;
    }

    public void UpdateDatabaseWithDataset(DataSet dataSet, string? targetDb = null)
    {
        if (targetDb is not null)
        {
            ChangeDatabase(targetDb);
        }

        for (var i = 0; i < dataSet.Tables.Count; i++)
        {
            var table = dataSet.Tables[i];
            var tableName = table.TableName;
            if (targetDb is not null) // than try create table 
            {
                var exist = (int)Connection
                    .CreateCommand()
                    .ExecuteScalar(
                        "IF EXISTS (" +
                        "SELECT 1 FROM INFORMATION_SCHEMA.TABLES " +
                        "WHERE TABLE_TYPE='BASE TABLE' AND " +
                        $"TABLE_NAME='{tableName}'" +
                        ") SELECT 1 ELSE SELECT 0;");
                if (exist == 0)
                {
                    Connection.CreateCommand().ExecuteNonQuery(SqlHelper.GetCreateTableSql(table));
                }
            }

            _adapter.SelectCommand = Connection.CreateCommand();
            _adapter.SelectCommand.CommandText = $"SELECT * FROM {tableName}";
            _commandBuilder.Dispose();
            _commandBuilder = new SqlCommandBuilder(_adapter);

            if (table.PrimaryKey.Any())
            {
                _adapter.InsertCommand = _commandBuilder.GetInsertCommand();
                _adapter.DeleteCommand = _commandBuilder.GetDeleteCommand();
                _adapter.UpdateCommand = _commandBuilder.GetUpdateCommand();
                _adapter.Update(dataSet, tableName);
                continue;
            }

            var cols = table.Columns;
            DataColumn idCol = null!;
            var hasIdentity = false;
            for (var j = 0; j < cols.Count; j++)
            {
                if (!hasIdentity && (hasIdentity |= cols[j].AutoIncrement))
                {
                    idCol = cols[j];
                }
            }

            using var cmd = _commandBuilder.GetInsertCommand();
            using var deleteCmd = Connection.CreateCommand();
            SqlParameter delCmdParam = null!;
            if (hasIdentity)
            {
                cmd.CommandText = cmd.CommandText
                    .InsertAfter($"[{tableName}] (", $"[{idCol.ColumnName}], ")
                    .InsertAfter("VALUES (", "@p0, ");
                cmd.Parameters.Insert(0, new SqlParameter("@p0", SqlDbType.Int, 0, idCol.ColumnName));
                deleteCmd.CommandText = $"DELETE FROM {tableName} WHERE {idCol.ColumnName} = @p0";
                delCmdParam = deleteCmd.Parameters.Add("@p0", SqlDbType.Int, 0, idCol.ColumnName)!;
            }

            if (hasIdentity) Connection.CreateCommand().ExecuteNonQuery($"SET IDENTITY_INSERT {tableName} ON");
            var rows = table.Rows;
            for (var j = 0; j < rows.Count; j++)
            {
                for (var k = 0; k < cmd.Parameters.Count; k++)
                {
                    cmd.Parameters[k].Value = rows[j].ItemArray[k];
                }

                if (hasIdentity)
                {
                    delCmdParam.Value = rows[j].ItemArray[0];
                    deleteCmd.ExecuteNonQuery();
                }
                cmd.ExecuteNonQuery();
            }

            if (hasIdentity) Connection.CreateCommand().ExecuteNonQuery($"SET IDENTITY_INSERT {tableName} OFF");
        }
    }

    public void Dispose()
    {
        _connection?.Dispose();
        _adapter.Dispose();
        _commandBuilder.Dispose();
    }
}