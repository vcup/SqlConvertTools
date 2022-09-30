using System.Data;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Data.SqlClient;
using SqlConvertTools.Extensions;

namespace SqlConvertTools.DbHandlers;

internal class SqlserverHandler : IDisposable
{
    private SqlConnection? _connection;
    private readonly SqlDataAdapter _adapter;
    private readonly SqlCommandBuilder _commandBuilder;

    public SqlserverHandler(string address, string password, string user = "sa")
    {
        _adapter = new SqlDataAdapter();
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
    public void FillDatabaseWithDataset(DataSet dataSet, string? targetDb = null)
    {
        if (targetDb is not null)
        {
            ChangeDatabase(targetDb);
        }

        for (var i = 0; i < dataSet.Tables.Count; i++)
        {
            var tableName = dataSet.Tables[i].TableName;
            var cmd = _commandBuilder.GetInsertCommand();
            var idCol = dataSet.Tables[i].Columns[0];
            cmd.CommandText = cmd.CommandText
                .InsertAfter($"[{tableName}] (", $"[{idCol.ColumnName}], ")
                .InsertAfter("VALUES (", "@p0, ");
            cmd.Parameters.Insert(0, new SqlParameter("@p0", SqlDbType.Int, 0, idCol.ColumnName));

            using var deleteCmd = Connection.CreateCommand();
            deleteCmd.CommandText = $"DELETE FROM {tableName} WHERE {idCol.ColumnName} = @p0";
            var delCmdParam = deleteCmd.Parameters.Add("@p0", SqlDbType.Int, 0, idCol.ColumnName)!;

            Connection.CreateCommand().ExecuteNonQuery($"SET IDENTITY_INSERT {tableName} ON");
            var rows = dataSet.Tables[i].Rows;
            for (var j = 0; j < rows.Count; j++)
            {
                delCmdParam.Value = rows[j].ItemArray[0];
                for (var k = 0; k < cmd.Parameters.Count; k++)
                {
                    cmd.Parameters[k].Value = rows[j].ItemArray[k];
                }

                deleteCmd.ExecuteNonQuery();
                cmd.ExecuteNonQuery();
            }

            Connection.CreateCommand().ExecuteNonQuery($"SET IDENTITY_INSERT {tableName} OFF");
        }
    }

    public void Dispose()
    {
        _connection?.Dispose();
    }
}