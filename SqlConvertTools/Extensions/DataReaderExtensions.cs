using System.Data;
using System.Reflection;
using Microsoft.Data.SqlClient;
using MySqlConnector;

namespace SqlConvertTools.Extensions;

public static class DataReaderExtensions
{
    public static void Dispose(this IDataReader reader, bool disposeConnection)
    {
        using (reader)
        {
            if (!disposeConnection) return;
            switch (reader)
            {
                case SqlDataReader sqlDataReader:
                {
                    var type = typeof(SqlDataReader);
                    var property = type.GetProperty("Connection", BindingFlags.Instance | BindingFlags.NonPublic);
                    using var connection = property!.GetValue(sqlDataReader) as SqlConnection;
                    break;
                }
                case MySqlDataReader mySqlDataReader:
                {
                    throw new NotImplementedException();
                }
            }
        }

    }
}