using System.Diagnostics.CodeAnalysis;
using System.IO.Hashing;
using System.Numerics;
using Database.Core.Execution;

namespace Database.Core.Functions;

public static class HashFunctions
{
    private static readonly XxHash3 xxHash3 = new XxHash3();

    public static Column<int> Hash(IReadOnlyList<IColumn> columns)
    {
        if (columns.Count == 1)
        {
            var column = columns[0].ValuesArray;
            if (HashSingleValues(column, out var hash))
            {
                return hash;
            }
            // TODO apply xxhash to all single value and multi value columns
        }

        if (columns.Count == 2)
        {
            return HashTwo(columns[0], columns[1]);
        }

        var rows = columns[0].Length;
        var hashes = InitializeResult(rows);

        for (var c = 1; c < columns.Count; c++)
        {
            var column = columns[c];
            for (var i = 0; i < rows; i++)
            {
                hashes[i] = hashes[i] * 31 + (column[i]?.GetHashCode() ?? 0);
            }
        }

        return new Column<int>("hash", hashes);
    }

    private static bool HashSingleValues(Array column, [NotNullWhen(true)] out Column<int>? hash)
    {
        if (column is int[] intColumn)
        {
            hash = HashOne(intColumn, BitConverter.GetBytes);
            return true;
        }
        if (column is long[] longColumn)
        {
            hash = HashOne(longColumn, BitConverter.GetBytes);
            return true;
        }
        if (column is float[] floatColumn)
        {
            hash = HashOne(floatColumn, BitConverter.GetBytes);
            return true;
        }
        if (column is double[] doubleColumn)
        {
            hash = HashOne(doubleColumn, BitConverter.GetBytes);
            return true;
        }

        hash = null;
        return false;
    }

    private static Column<int> HashOne<T>(T[] values, Func<T, byte[]> getBytes)
    {
        var rows = values.Length;
        var hashes = new int[rows];

        for (var i = 0; i < rows; i++)
        {
            var value = values[i];

            xxHash3.Reset();
            var intAsBytes = getBytes(value);
            xxHash3.Append(intAsBytes);
            var hash = xxHash3.GetCurrentHashAsUInt64();
            hashes[i] = Xxh3LowInt(hash);
        }

        return new Column<int>("hash", hashes);
    }

    private static int Xxh3LowInt(ulong hash)
    {
        return (int)(hash & 0xFFFFFFFF);
    }

    public static Column<int> HashTwo(IColumn one, IColumn two)
    {
        var rows = one.Length;
        var hashes = InitializeResult(rows);

        for (var i = 0; i < rows; i++)
        {
            var hash = hashes[i];
            hash = hash * 31 + (one[i]?.GetHashCode() ?? 0);
            hash = hash * 31 + (two[i]?.GetHashCode() ?? 0);
            hashes[i] = hash;
        }

        return new Column<int>("hash", hashes);
    }

    public static int[] Hash(List<Array> keys, bool[] mask)
    {
        var rows = mask.Length;
        var hashes = InitializeResult(rows);

        if (keys.Count == 1)
        {
            // TODO mask?
            if (HashSingleValues(keys[0], out var hash))
            {
                return hash.Values;
            }
        }

        for (var i = 0; i < rows; i++)
        {
            if (!mask[i])
            {
                continue;
            }

            for (var c = 1; c < keys.Count; c++)
            {
                hashes[i] = hashes[i] * 31 + (keys[c].GetValue(i)?.GetHashCode() ?? 0);
            }
        }

        return hashes;
    }

    private static int[] InitializeResult(int rows)
    {
        var hashes = new int[rows];
        for (var i = 0; i < rows; i++)
        {
            hashes[i] = 17;
        }
        return hashes;
    }
}
