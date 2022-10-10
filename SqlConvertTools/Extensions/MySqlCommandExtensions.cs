using System.Data;
using MySql.Data.MySqlClient;

namespace SqlConvertTools.Extensions;

public static class MySqlCommandExtensions
{
    public static void ExecuteNonQuery(this MySqlCommand command, string cmdText, bool disposing = true)
    {
        command.CommandText = cmdText;
        command.ExecuteNonQuery();
        if (disposing)
        {
            command.Dispose();
        }
    }

    public static MySqlDataReader ExecuteReader(this MySqlCommand command, string cmdText, bool disposing = true)
    {
        command.CommandText = cmdText;
        var reader = command.ExecuteReader();
        if (disposing)
        {
            command.Dispose();
        }

        return reader;
    }

    public static MySqlDataReader ExecuteReader(this MySqlCommand command, CommandBehavior commandBehavior, string cmdText,
        bool disposing = true)
    {
        command.CommandText = cmdText;
        var reader = command.ExecuteReader(commandBehavior);
        if (disposing)
        {
            command.Dispose();
        }

        return reader;
    }

    public static object ExecuteScalar(this MySqlCommand command, string cmdText, bool disposing = true)
    {
        command.CommandText = cmdText;
        var result = command.ExecuteScalar();
        if (disposing)
        {
            command.Dispose();
        }

        return result;
    }
}