using Database.Core.Execution;

namespace Database.Core.Functions;

public static class HashFunctions
{
    public static Column<int> Hash(IReadOnlyList<IColumn> columns)
    {
        if (columns.Count == 2)
        {
            return HashTwo(columns[0], columns[1]);
        }

        var rows = columns[0].Length;
        var values = InitializeResult(rows);

        for (var c = 1; c < columns.Count; c++)
        {
            var column = columns[c];
            for (var i = 0; i < rows; i++)
            {
                values[i] = values[i] * 31 + (column[i]?.GetHashCode() ?? 0);
            }
        }

        return new Column<int>("hash", values);
    }

    public static Column<int> HashTwo(IColumn one, IColumn two)
    {
        var rows = one.Length;
        var values = InitializeResult(rows);

        for (var i = 0; i < rows; i++)
        {
            var hash = values[i];
            hash = hash * 31 + (one[i]?.GetHashCode() ?? 0);
            hash = hash * 31 + (two[i]?.GetHashCode() ?? 0);
            values[i] = hash;
        }

        return new Column<int>("hash", values);
    }

    private static int[] InitializeResult(int rows)
    {
        var values = new int[rows];
        for (var i = 0; i < rows; i++)
        {
            values[i] = 17;
        }
        return values;
    }

}
