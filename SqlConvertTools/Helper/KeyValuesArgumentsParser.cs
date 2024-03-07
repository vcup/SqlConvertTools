using System.Collections.Immutable;
using System.CommandLine.Parsing;

namespace SqlConvertTools.Helper;

public static class KeyValuesArgumentsParser
{
    public static IReadOnlyDictionary<string, string[]> Parse(ArgumentResult args) => Parse(args, ':');

    public static IReadOnlyDictionary<string, string[]> Parse(ArgumentResult args, char sep)
    {
        var parseResult = new Dictionary<string, List<string>>();
        var parsingSplit = args.Tokens
            .Select(i => i.Value.Split(':'))
            .Select(i => (i[0], i[1..]));
        foreach (var (key, value) in parsingSplit)
        {
            if (parseResult.TryGetValue(key, out var list))
            {
                (list as List<string>)!.AddRange(value);
            }
            else
            {
                parseResult[key] = new List<string>(value);
            }
        }

        return parseResult.ToImmutableDictionary(
            i => i.Key,
            i => i.Value.ToArray()
        );
    }
}