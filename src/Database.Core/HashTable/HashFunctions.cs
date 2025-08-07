using System.IO.Hashing;
using System.Text;
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
            return new Column<int>("hashes", HashSingleValues(column));
        }

        var rows = columns[0].Length;
        var hashes = new ulong[rows];

        for (var c = 1; c < columns.Count; c++)
        {
            var column = columns[c];
            HashAndMix(column.ValuesArray, hashes);
        }

        var res = AvalancheLowInt(hashes);
        return new Column<int>("hash", res);
    }

    private static int[] HashSingleValues(Array column)
    {
        if (column is int[] intColumn)
        {
            return HashOne(intColumn, BitConverter.GetBytes);
        }
        if (column is long[] longColumn)
        {
            return HashOne(longColumn, BitConverter.GetBytes);
        }
        if (column is float[] floatColumn)
        {
            return HashOne(floatColumn, BitConverter.GetBytes);
        }
        if (column is double[] doubleColumn)
        {
            return HashOne(doubleColumn, BitConverter.GetBytes);
        }
        if (column is decimal[] decimals)
        {
            return HashOne(decimals, DecimalToBytes);
        }
        if (column is string[] strings)
        {
            return HashOne(strings, Encoding.UTF8.GetBytes);
        }
        if (column is bool[] bools)
        {
            return HashOne(bools, BitConverter.GetBytes);
        }
        if (column is DateTime[] dates)
        {
            return HashOne(dates, d => BitConverter.GetBytes(d.Ticks));
        }
        if (column is TimeSpan[] ts)
        {
            return HashOne(ts, t => BitConverter.GetBytes(t.Ticks));
        }
        throw new NotImplementedException($"HashSingleValues not implemented for type {column.GetType().Name}");
    }

    private static void HashAndMix(Array column, ulong[] hashes)
    {
        if (column is int[] intColumn)
        {
            HashAndMix(intColumn, BitConverter.GetBytes, hashes);
            return;
        }
        if (column is long[] longColumn)
        {
            HashAndMix(longColumn, BitConverter.GetBytes, hashes);
            return;
        }
        if (column is float[] floatColumn)
        {
            HashAndMix(floatColumn, BitConverter.GetBytes, hashes);
            return;
        }
        if (column is double[] doubleColumn)
        {
            HashAndMix(doubleColumn, BitConverter.GetBytes, hashes);
            return;
        }
        if (column is decimal[] decimals)
        {
            HashAndMix(decimals, DecimalToBytes, hashes);
            return;
        }
        if (column is string[] strings)
        {
            HashAndMix(strings, Encoding.UTF8.GetBytes, hashes);
            return;
        }
        if (column is bool[] bools)
        {
            HashAndMix(bools, BitConverter.GetBytes, hashes);
            return;
        }
        if (column is DateTime[] dates)
        {
            HashAndMix(dates, d => BitConverter.GetBytes(d.Ticks), hashes);
            return;
        }
        if (column is TimeSpan[] ts)
        {
            HashAndMix(ts, t => BitConverter.GetBytes(t.Ticks), hashes);
            return;
        }
        throw new NotImplementedException($"HashAndMix not implemented for type {column.GetType().Name}");
    }

    private static byte[] DecimalToBytes(decimal d)
    {
        return BitConverter.GetBytes((double)Convert.ChangeType(d, typeof(double)));
    }

    private static int[] HashOne<T>(T[] values, Func<T, byte[]> getBytes)
    {
        var rows = values.Length;
        var hashes = new int[rows];

        for (var i = 0; i < rows; i++)
        {
            var value = values[i];

            xxHash3.Reset();
            var asBytes = getBytes(value);
            xxHash3.Append(asBytes);
            var hash = xxHash3.GetCurrentHashAsUInt64();
            hashes[i] = Xxh3LowInt(hash);
        }

        return hashes;
    }

    private static void HashAndMix<T>(T[] values, Func<T, byte[]> getBytes, ulong[] hashes)
    {
        var rows = values.Length;

        for (var i = 0; i < rows; i++)
        {
            var value = values[i];

            xxHash3.Reset();
            var asBytes = getBytes(value);
            xxHash3.Append(asBytes);
            var hash = xxHash3.GetCurrentHashAsUInt64();
            hashes[i] = Mix2(hashes[i], hash);
        }
    }

    private static int Xxh3LowInt(ulong hash)
    {
        return (int)(hash & 0xFFFFFFFF);
    }

    private static ulong Mix2(ulong a, ulong b)
    {
        // Constants from XXH3 / Murmur
        const ulong PRIME64_1 = 11400714785074694791ul;
        const ulong PRIME64_2 = 14029467366897019727ul;

        ulong hash = a + PRIME64_1;
        hash ^= b;
        hash = (hash ^ (hash >> 33)) * PRIME64_2;
        hash ^= hash >> 29;
        return hash;
    }

    private static int[] AvalancheLowInt(ulong[] hashes)
    {
        var result = new int[hashes.Length];
        for (var i = 0; i < hashes.Length; i++)
        {
            result[i] = Xxh3LowInt(Avalanche(hashes[i]));
        }

        return result;
    }

    private static ulong Avalanche(ulong h)
    {
        h ^= h >> 33;
        h *= 0xff51afd7ed558ccd;
        h ^= h >> 33;
        h *= 0xc4ceb9fe1a85ec53;
        h ^= h >> 33;
        return h;
    }

    public static int[] Hash(List<Array> columns, bool[] mask)
    {
        // TODO mask?

        if (columns.Count == 1)
        {
            var column = columns[0];
            return HashSingleValues(column);
        }

        var rows = columns[0].Length;
        var hashes = new ulong[rows];

        for (var c = 1; c < columns.Count; c++)
        {
            var column = columns[c];
            HashAndMix(column, hashes);
        }
        return AvalancheLowInt(hashes);
    }
}
