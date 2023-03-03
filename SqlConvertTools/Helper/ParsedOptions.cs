using SqlConvertTools.Utils;

namespace SqlConvertTools.Helper;

internal static class ParsedOptions
{
    public static string? SourceUserName { get; set; }

    public static string? TargetUserName { get; set; }

    public static string? Password { get; set; }

    public static string? SourcePassword { get; set; }

    public static string? TargetPassword { get; set; }

    public static string[] IgnoreTables { get; set; } = null!;

    public static IReadOnlyDictionary<string, IEnumerable<string>> IgnoreDatabaseTables { get; set; } = null!;

    public static IReadOnlyDictionary<string, string> CustomDatabaseNames { get; set; } = null!;

    public static bool TrustSourceCert { get; set; }

    public static bool OverrideTableIfExist { get; set; }

    public static int ParallelTablesTransfer { get; set; }

    public static IReadOnlyCollection<CustomColumnDataType> CustomColumnDataTypes { get; set; } = null!;
}