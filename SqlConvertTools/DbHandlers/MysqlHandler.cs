using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using MySql.Data.MySqlClient;
using SqlConvertTools.Extensions;

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
        var sql = SqlHelper.GetCreateTableSql(table);
        sql = sql.Replace("IDENTITY(0,1)", "AUTO_INCREMENT")
            .Replace("[", "")
            .Replace("]", "");
        Connection.CreateCommand().ExecuteNonQuery(sql);
    }

    public int UpdateDatabaseWith(DataTable table)
    {
        throw new NotImplementedException();
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