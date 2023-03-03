using System.Data;
using System.Text;

namespace SqlConvertTools.Helper;

public static class SqlHelper
{
    /// copy from https://stackoverflow.com/questions/1348712/creating-a-sql-server-table-from-a-c-sharp-datatable
    /// LoneBunny answered Dec 2, 2014 at 4:49
    /// <summary>
    /// Inspects a DataTable and return a SQL string that can be used to CREATE a TABLE in SQL Server.
    /// </summary>
    /// <param name="table">System.Data.DataTable object to be inspected for building the SQL CREATE TABLE statement.</param>
    /// <returns>String of SQL</returns>
    public static string GetCreateTableSqlForSqlserver(DataTable table)
    {
        var sql = new StringBuilder();
        var alterSql = new StringBuilder();

        sql.Append($"CREATE TABLE [{table.TableName}] (");

        for (var i = 0; i < table.Columns.Count; i++)
        {
            var isNumeric = false;
            var usesColumnDefault = true;

            sql.Append($"\n\t[{table.Columns[i].ColumnName}] ");

            switch (table.Columns[i].DataType.ToString())
            {
                case "System.Guid":
                    sql.Append("uniqueidentifier");
                    break;
                case "System.Int16":
                    sql.Append("smallint");
                    isNumeric = true;
                    break;
                case "System.Int32":
                    sql.Append("int");
                    isNumeric = true;
                    break;
                case "System.Int64":
                    sql.Append("bigint");
                    isNumeric = true;
                    break;
                case "System.DateTime":
                    sql.Append("datetime");
                    usesColumnDefault = false;
                    break;
                case "System.String":
                    if (table.Columns[i].MaxLength is > 4000 or -1)
                    {
                        sql.Append(@"nvarchar(MAX)");
                    }
                    else
                    {
                        sql.Append($@"nvarchar({table.Columns[i].MaxLength})");
                    }

                    break;
                case "System.Single":
                    sql.Append("single");
                    isNumeric = true;
                    break;
                case "System.Double":
                    sql.Append("float");
                    isNumeric = true;
                    break;
                case "System.Decimal":
                    sql.AppendFormat("decimal(18, 6)");
                    isNumeric = true;
                    break;
                case "System.Boolean":
                    sql.Append("bit");
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            if (table.Columns[i].AutoIncrement)
            {
                sql.Append($" IDENTITY({table.Columns[i].AutoIncrementSeed},{table.Columns[i].AutoIncrementStep})");
            }
            else
            {
                // DataColumns will add a blank DefaultValue for any AutoIncrement column. 
                // We only want to create an ALTER statement for those columns that are not set to AutoIncrement. 
                if (table.Columns[i].DefaultValue is not null)
                {
                    if (usesColumnDefault)
                    {
                        if (isNumeric)
                        {
                            alterSql.AppendFormat(
                                "\nALTER TABLE {0} ADD CONSTRAINT [DF_{0}_{1}]  DEFAULT ({2}) FOR [{1}];",
                                table.TableName,
                                table.Columns[i].ColumnName,
                                table.Columns[i].DefaultValue);
                        }
                        else
                        {
                            alterSql.AppendFormat(
                                "\nALTER TABLE {0} ADD CONSTRAINT [DF_{0}_{1}]  DEFAULT ('{2}') FOR [{1}];",
                                table.TableName,
                                table.Columns[i].ColumnName,
                                table.Columns[i].DefaultValue);
                        }
                    }
                    else
                    {
                        // Default values on Date columns, e.g., "DateTime.Now" will not translate to SQL.
                        // This inspects the caption for a simple XML string to see if there is a SQL compliant default value, e.g., "GETDATE()".
                        try
                        {
                            var xml = new System.Xml.XmlDocument();

                            xml.LoadXml(table.Columns[i].Caption);

                            alterSql.AppendFormat(
                                "\nALTER TABLE {0} ADD CONSTRAINT [DF_{0}_{1}]  DEFAULT ({2}) FOR [{1}];",
                                table.TableName,
                                table.Columns[i].ColumnName,
                                xml.GetElementsByTagName("defaultValue")[0]!.InnerText);
                        }
                        catch
                        {
                            // Handle
                        }
                    }
                }
            }

            if (!table.Columns[i].AllowDBNull)
            {
                sql.Append(" NOT NULL");
            }

            sql.Append(',');
        }

        if (table.PrimaryKey.Length > 0)
        {
            var primaryKeySql = new StringBuilder();

            primaryKeySql.Append($"\n\tCONSTRAINT PK_{table.TableName} PRIMARY KEY (");

            foreach (var t in table.PrimaryKey)
            {
                primaryKeySql.Append($"{t.ColumnName},");
            }

            primaryKeySql.Remove(primaryKeySql.Length - 1, 1);
            primaryKeySql.Append(')');

            sql.Append(primaryKeySql);
        }
        else
        {
            sql.Remove(sql.Length - 1, 1);
        }

        //sql.Append(alterSql);
        sql.Append("\n);\n");

        return sql.ToString();
    }

    public static string GetCreateTableSqlForMySql(DataTable table)
    {
        var sql = new StringBuilder($"Create Table `{table.TableName}` (");
        sql.Append('\n');

        var rowTotalSize = 0;
        foreach (DataColumn column in table.Columns)
        {
            sql.Append($"\t`{column.ColumnName}`");
            sql.Append($" {GetCustomDataType(column) ?? NetTypeMapToMySqlDataType(column)} ");
            if (column.AutoIncrement) sql.Append("AUTO_INCREMENT ");
            if (column.AutoIncrementSeed is not 0) sql.Append($"= {column.AutoIncrementSeed}");
            if (!column.AllowDBNull && column.DefaultValue is not { })
            {
                sql.Append($"DEFAULT {column.DefaultValue}");
            }

            sql.Append(column.AllowDBNull ? "DEFAULT NULL" : "NOT NULL");

            sql.Append(",\n");
        }

        if (table.PrimaryKey.Any())
        {
            sql.Append($"\n\tCONSTRAINT PK_{table.TableName} PRIMARY KEY (");

            foreach (var t in table.PrimaryKey)
            {
                sql.Append($"{t.ColumnName},");
            }

            sql.Remove(sql.Length - 1, 1);
            sql.Append(')');
        }
        else
        {
            sql.Remove(sql.Length - 2, 1);
        }

        sql.AppendLine("\n);");

        return sql.ToString();

        string? GetCustomDataType(DataColumn column)
        {
            var customDataType = ParsedOptions.CustomColumnDataTypes
                .FirstOrDefault(i =>
                    i.Column == column.ColumnName
                    && (i.Table is null || i.Table == column.Table!.TableName)
                    && (i.Database is null || i.Database == column.Table!.DataSet!.DataSetName));
            return customDataType?.DataType;
        }

        string NetTypeMapToMySqlDataType(DataColumn column)
        {
            // avoid Row size too large.
            // reference https://mariadb.com/kb/en/troubleshooting-row-size-too-large-errors-with-innodb/
            rowTotalSize += column.MaxLength;
            switch (column.DataType.FullName)
            {
                case "System.Boolean":
                    return "boolean";
                case "System.Byte":
                    return "tinyint unsigned";
                case "System.Byte[]":
                    return "binary";
                case "System.DateTime":
                case "System.DateTimeOffset":
                    return "datetime";
                case "System.Decimal":
                    return "decimal";
                case "System.Double":
                    return "double";
                case "System.Guid":
                    return "char(36)";
                case "System.Int16":
                    return "smallint";
                case "System.Int32":
                    return "int";
                case "System.Int64":
                    return "bigint";
                case "System.SByte":
                    return "tinyint";
                case "System.Single":
                    return "float";
                case "System.String":
                    return rowTotalSize + column.MaxLength > 15000
                           || column.MaxLength is > 4000 or -1
                        ? "longtext"
                        : $"varchar({column.MaxLength})";
                case "System.TimeSpan":
                    return "time";
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}