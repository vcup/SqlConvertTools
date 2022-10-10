using System.Data;
using System.Diagnostics.CodeAnalysis;

namespace SqlConvertTools.Helper;

public static class DbHelper
{

    public static bool TryGetIdentityColumn(DataColumnCollection cols, [NotNullWhen(true)] out DataColumn? idCol)
    {
        idCol = null;
        for (var j = 0; j < cols.Count; j++)
        {
            if (!cols[j].AutoIncrement) continue;
            idCol = cols[j];
            return true;
        }

        return false;
    }
}