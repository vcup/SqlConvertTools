using System.Data;
using System.Text.RegularExpressions;

namespace SqlConvertTools.Utils;

public class DataColumnDefaultValueHandler
{
    public DataColumnDefaultValueHandler(string? text)
    {
        Origin = text;
        if (string.IsNullOrEmpty(text))
        {
            DefaultValue = DataColumnDefaultValueEnum.Null;
            return;
        }

        text = TrimBracket(text);
        var numberRex = new Regex(@"^\d$");
        var stringRex = new Regex("^(N)?(\"|'|`).*(\"|'|`)$");
        DefaultValue = text.ToLowerInvariant() switch
        {
            "false" or "true" => DataColumnDefaultValueEnum.Constant,
            _ when numberRex.IsMatch(text) => DataColumnDefaultValueEnum.Constant,
            _ when stringRex.IsMatch(text) => DataColumnDefaultValueEnum.String,
            "current_timestamp" or "getdate()" => DataColumnDefaultValueEnum.CurDateTime,
            "uuid()" or "newid()" => DataColumnDefaultValueEnum.Guid,
            "rand()" => DataColumnDefaultValueEnum.Random,
            _ => DataColumnDefaultValueEnum.Unrecognized,
        };
        Normalized = text;
    }

    private static string TrimBracket(string input)
    {
        while (true)
        {
            if (input.Length < 3 ||
                input[0] is not ('[' or '(' or '{') ||
                input[^1] is not (']' or ')' or '}')
               ) return input;
            input = input[1..^1];
        }
    }

    public DataColumnDefaultValueEnum DefaultValue { get; }

    public string GetDefaultValue(DbmsType dbmsType) => dbmsType switch
    {
        DbmsType.SqlServer => GetForSqlServer(),
        DbmsType.MySql => GetForMySql(),
        _ => throw new ArgumentOutOfRangeException(nameof(dbmsType), dbmsType, null)
    };
    private string GetForSqlServer() => DefaultValue switch
    {
        DataColumnDefaultValueEnum.Constant => Normalized!,
        DataColumnDefaultValueEnum.String => Normalized!,
        DataColumnDefaultValueEnum.CurDateTime => "getdate()",
        DataColumnDefaultValueEnum.Guid => "newid()",
        DataColumnDefaultValueEnum.Random => "rand()",
        DataColumnDefaultValueEnum.Null => "null",
        DataColumnDefaultValueEnum.Unrecognized => Normalized!,
        _ => throw new ArgumentOutOfRangeException()
    };

    private string GetForMySql() => DefaultValue switch
    {
        DataColumnDefaultValueEnum.Constant => Normalized!,
        DataColumnDefaultValueEnum.String => Normalized![0] is 'N' ? Normalized[1..] : Normalized,
        DataColumnDefaultValueEnum.CurDateTime => "CURRENT_TIMESTAMP",
        DataColumnDefaultValueEnum.Guid => "UUID()",
        DataColumnDefaultValueEnum.Random => "RAND()",
        DataColumnDefaultValueEnum.Null => "null",
        DataColumnDefaultValueEnum.Unrecognized => Normalized!,
        _ => throw new ArgumentOutOfRangeException()
    };

    public string? Origin { get; }
    public string? Normalized { get; }
}