using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using MySql.Data.MySqlClient;
using SqlConvertTools.Extensions;

namespace SqlConvertTools.DbHandlers;

public class MysqlHandler : IDbHandler
{
    private MySqlConnection? _connection;
    private readonly MySqlDataAdapter _adapter;


    public MysqlHandler(string address, string password, string user = "sa") : this(new MySqlConnectionStringBuilder
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

    private MySqlConnection Connection => _connection ??= new MySqlConnection(ConnectionStringBuilder.ConnectionString);

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
                ConnectionStringBuilder.Database = "master";
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
        ChangeDatabase(dbname);
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
                ConnectionStringBuilder.Database = "master";
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
        ChangeDatabase(dbname);
        return flag;
    }

    public void ChangeDatabase(string dbname)
    {
        using (var reader = Connection.CreateCommand()
                   .ExecuteReader(@$"Select name From sys.databases Where database_id > 4 And name = '{dbname}'"))
        {
            if (dbname is not "mysql" && !reader.HasRows)
            {
                reader.Dispose();
                Connection.CreateCommand().ExecuteNonQuery($"Create Database {dbname}");
            }
        }

        Connection.ChangeDatabase(dbname);
    }

    public DataSet FillDataset(string tableName, DataSet? dataSet, out int count)
    {
        throw new NotImplementedException();
    }

    public DataSet FillDatasetWithoutData(string tableName, DataSet? dataSet)
    {
        throw new NotImplementedException();
    }

    public IEnumerable<string> GetDatabases(bool excludeSysDb = true)
    {
        throw new NotImplementedException();
    }

    public IEnumerable<string> GetTableNames()
    {
        throw new NotImplementedException();
    }

    public void CreateTable(DataTable table)
    {
        throw new NotImplementedException();
    }

    public int UpdateDatabaseWith(DataTable table)
    {
        throw new NotImplementedException();
    }

    public int GetRowCount(string tableName)
    {
        throw new NotImplementedException();
    }
}