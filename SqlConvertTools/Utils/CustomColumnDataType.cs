namespace SqlConvertTools.Utils;

public class CustomColumnDataType
{
    public string DataType { get; init; } = string.Empty;

    public string Column { get; init; } = string.Empty;

    public string? Table { get; init; }

    public string? Database { get; init; }
}