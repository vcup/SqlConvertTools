using System.Data;
using Microsoft.Data.SqlClient;

namespace SqlConvertTools.Extensions;

public static class SqlCommandExtensions
{
    public static void ExecuteNonQuery(this SqlCommand command, string cmdText, bool disposing = true)
    {
        command.CommandText = cmdText;
        command.ExecuteNonQuery();
        if (disposing)
        {
            command.Dispose();
        }
    }

    public static SqlDataReader ExecuteReader(this SqlCommand command, string cmdText, bool disposing = true)
    {
        command.CommandText = cmdText;
        var reader = command.ExecuteReader();
        if (disposing)
        {
            command.Dispose();
        }

        return reader;
    }

    public static SqlDataReader ExecuteReader(this SqlCommand command, CommandBehavior commandBehavior, string cmdText,
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
}