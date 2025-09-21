using System.IO.Hashing;
using System.Text;
using Database.Core.Execution;
using Database.Core.Types;

namespace Database.Core.Functions;

public static class HashFunctions
{
    private static readonly XxHash3 xxHash3 = new XxHash3();

    public delegate bool TryWriteBytes<T>(Span<byte> destination, T input, out int bytesWritten);

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
        byte[] bytes;
        if (column is int[] intColumn)
        {
            bytes = new byte[sizeof(int)];
            return HashOne(intColumn, bytes, BitConverter.TryWriteBytes);
        }
        if (column is long[] longColumn)
        {
            bytes = new byte[sizeof(long)];
            return HashOne(longColumn, bytes, BitConverter.TryWriteBytes);
        }
        if (column is float[] floatColumn)
        {
            bytes = new byte[sizeof(float)];
            return HashOne(floatColumn, bytes, BitConverter.TryWriteBytes);
        }
        if (column is double[] doubleColumn)
        {
            bytes = new byte[sizeof(double)];
            return HashOne(doubleColumn, bytes, BitConverter.TryWriteBytes);
        }
        if (column is Decimal15[] decimals)
        {
            bytes = new byte[sizeof(long)];
            return HashOne(decimals, bytes, DecimalToBytes);
        }
        if (column is string[] strings)
        {
            bytes = new byte[4096];
            return HashOne(strings, bytes, StringToBytes);
        }
        if (column is bool[] bools)
        {
            bytes = new byte[sizeof(bool)];
            return HashOne(bools, bytes, BitConverter.TryWriteBytes);
        }
        if (column is DateTime[] dates)
        {
            bytes = new byte[sizeof(long)];
            return HashOne(dates, bytes, DateTimeToBytes);
        }
        if (column is TimeSpan[] ts)
        {
            bytes = new byte[sizeof(long)];
            return HashOne(ts, bytes, TimeSpanToBytes);
        }
        throw new NotImplementedException($"HashSingleValues not implemented for type {column.GetType().Name}");
    }

    private static void HashAndMix(Array column, ulong[] hashes)
    {
        byte[] bytes;
        if (column is int[] intColumn)
        {
            bytes = new byte[sizeof(int)];
            HashAndMix(intColumn, bytes, BitConverter.TryWriteBytes, hashes);
            return;
        }
        if (column is long[] longColumn)
        {
            bytes = new byte[sizeof(long)];
            HashAndMix(longColumn, bytes, BitConverter.TryWriteBytes, hashes);
            return;
        }
        if (column is float[] floatColumn)
        {
            bytes = new byte[sizeof(float)];
            HashAndMix(floatColumn, bytes, BitConverter.TryWriteBytes, hashes);
            return;
        }
        if (column is double[] doubleColumn)
        {
            bytes = new byte[sizeof(double)];
            HashAndMix(doubleColumn, bytes, BitConverter.TryWriteBytes, hashes);
            return;
        }
        if (column is Decimal15[] decimals)
        {
            bytes = new byte[sizeof(long)];
            HashAndMix(decimals, bytes, DecimalToBytes, hashes);
            return;
        }
        if (column is string[] strings)
        {
            bytes = new byte[4096];
            HashAndMix(strings, bytes, StringToBytes, hashes);
            return;
        }
        if (column is bool[] bools)
        {
            bytes = new byte[sizeof(bool)];
            HashAndMix(bools, bytes, BitConverter.TryWriteBytes, hashes);
            return;
        }
        if (column is DateTime[] dates)
        {
            bytes = new byte[sizeof(long)];
            HashAndMix(dates, bytes, DateTimeToBytes, hashes);
            return;
        }
        if (column is TimeSpan[] ts)
        {
            bytes = new byte[sizeof(long)];
            HashAndMix(ts, bytes, TimeSpanToBytes, hashes);
            return;
        }
        throw new NotImplementedException($"HashAndMix not implemented for type {column.GetType().Name}");
    }

    private static bool TimeSpanToBytes(Span<byte> dest, TimeSpan timeSpan, out int bytesWritten)
    {
        bytesWritten = sizeof(long);
        return BitConverter.TryWriteBytes(dest, timeSpan.Ticks);
    }

    private static bool DateTimeToBytes(Span<byte> dest, DateTime dateTime, out int bytesWritten)
    {
        bytesWritten = sizeof(long);
        return BitConverter.TryWriteBytes(dest, dateTime.Ticks);
    }

    private static bool StringToBytes(Span<byte> dest, string s, out int bytesWritten)
    {
        // TODO if the string is too large we need to loop over it multiple times
        // to include all pieces in the hash
        return Encoding.UTF8.TryGetBytes(s, dest, out bytesWritten);
    }

    private static bool DecimalToBytes(Span<byte> dest, Decimal15 d)
    {
        return BitConverter.TryWriteBytes(dest, d.Value);
    }

    private static int[] HashOne<T>(T[] values, byte[] buff, Func<Span<byte>, T, bool> writeBytes)
    {
        return HashOne(values, buff, Inner);

        bool Inner(Span<byte> dest, T input, out int bytesWritten)
        {
            bytesWritten = dest.Length;
            return writeBytes(dest, input);
        }
    }

    private static int[] HashOne<T>(T[] values, byte[] buff, TryWriteBytes<T> writeBytes)
    {
        var rows = values.Length;
        var hashes = new int[rows];

        for (var i = 0; i < rows; i++)
        {
            var value = values[i];

            xxHash3.Reset();
            if (!writeBytes(buff, value, out var bytesWritten))
            {
                throw new Exception("Could not write bytes");
            }
            xxHash3.Append(buff.AsSpan().Slice(0, bytesWritten));
            var hash = xxHash3.GetCurrentHashAsUInt64();
            hashes[i] = Xxh3LowInt(hash);
        }

        return hashes;
    }

    private static void HashAndMix<T>(T[] values, byte[] buff, Func<Span<byte>, T, bool> writeBytes, ulong[] hashes)
    {
        HashAndMix(values, buff, Inner, hashes);
        return;

        bool Inner(Span<byte> dest, T input, out int bytesWritten)
        {
            bytesWritten = dest.Length;
            return writeBytes(dest, input);
        }
    }


    private static void HashAndMix<T>(T[] values, byte[] buff, TryWriteBytes<T> writeBytes, ulong[] hashes)
    {
        var rows = values.Length;

        for (var i = 0; i < rows; i++)
        {
            var value = values[i];

            xxHash3.Reset();
            if (!writeBytes(buff, value, out var written))
            {
                throw new Exception("Could not write bytes");
            }
            xxHash3.Append(buff.AsSpan().Slice(0, written));
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
        if (columns.Count == 1)
        {
            var column = columns[0];
            column = FillArrayIfNullableType(column, mask);
            return HashSingleValues(column);
        }

        var rows = columns[0].Length;
        var hashes = new ulong[rows];

        for (var c = 1; c < columns.Count; c++)
        {
            var column = columns[c];
            column = FillArrayIfNullableType(column, mask);
            HashAndMix(column, hashes);
        }
        return AvalancheLowInt(hashes);
    }

    private static Array FillArrayIfNullableType(Array column, bool[] mask)
    {
        if (column is string[] input)
        {
            var result = new string[column.Length];
            for (var i = 0; i < column.Length; i++)
            {
                result[i] = mask[i] ? input[i] : string.Empty;
            }

            return result;
        }

        return column;
    }

}
