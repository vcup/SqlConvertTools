namespace SqlConvertTools.Extensions;

public static class StringExtension
{
    public static string InsertAfter(this string @string, string str, string insertString,
        StringComparison stringComparison = StringComparison.Ordinal)
    {
        var i = @string.IndexOf(str, stringComparison) + str.Length;
        return @string.Insert(i, insertString);
    }
}