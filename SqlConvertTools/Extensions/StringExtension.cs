namespace SqlConvertTools.Extensions;

public static class StringExtension
{
    public static string InsertAfter(this string @string, string str, string insertString)
    {
        var i = @string.IndexOf(str, StringComparison.Ordinal) + str.Length;
        return @string.Insert(i, insertString);
    }
}