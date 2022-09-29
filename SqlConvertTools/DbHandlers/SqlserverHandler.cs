using System.Data;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Data.SqlClient;
using SqlConvertTools.Extensions;

namespace SqlConvertTools.DbHandlers;

internal class SqlserverHandler : IDisposable
{
    private SqlConnection? _connection;

    public SqlserverHandler(string address, string password, string user = "sa")
    {
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

    public void Dispose()
    {
        _connection?.Dispose();
    }
}