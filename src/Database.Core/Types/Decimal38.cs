using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Numerics;
using System.Reflection;
using System.Reflection.Emit;

namespace Database.Core.Types;


[DebuggerDisplay("{AsDecimal()}")]
public readonly struct Decimal38 : INumber<Decimal38>, IMinMaxValue<Decimal38>, IConvertible
{
    public Int128 Value { get; private init; }

    private const int scale = 6;
    private const int factor = 1_000_000; // 10^scale
    private const int half_factor = 1_000;

    public Decimal38(float value)
    {
        Value = Int128.CreateTruncating(value * factor);
    }

    public Decimal38(double value)
    {
        Value = Int128.CreateTruncating(value * factor);
    }

    public Decimal38(decimal value)
    {
        Value = Int128.CreateTruncating(value * factor);
    }

    public Decimal38(int value)
    {
        Value = Int128.CreateTruncating(value * factor);
    }

    public Decimal38(long value)
    {
        Value = Int128.CreateTruncating(value * factor);
    }

    public Decimal38(Decimal15 value)
    {
        Value = Int128.CreateTruncating(value.Value * 10_000);
    }

    public static Decimal38 FromScaled(Int128 value)
    {
        return new Decimal38
        {
            Value = value,
        };
    }

    public decimal AsDecimal()
    {
        var res = (decimal)Value / factor;
        return Math.Round(res, scale);
    }

    public static Decimal38 One => new(1);
    public static int Radix => 2;
    public static Decimal38 Zero => new(0);

    public static Decimal38 operator +(Decimal38 left, Decimal38 right)
    {
        return FromScaled(left.Value + right.Value);
    }

    public static Decimal38 operator -(Decimal38 left, Decimal38 right)
    {
        return FromScaled(left.Value - right.Value);
    }

    public static Decimal38 operator -(Decimal38 value)
    {
        return FromScaled(-value.Value);
    }

    public static Decimal38 operator +(Decimal38 value)
    {
        return value;
    }

    public static Decimal38 Abs(Decimal38 value)
    {
        return FromScaled(Int128.Abs(value.Value));
    }

    public static bool operator ==(Decimal38 left, Decimal38 right)
    {
        return left.Value == right.Value;
    }

    public static bool operator !=(Decimal38 left, Decimal38 right)
    {
        return left.Value != right.Value;
    }

    public static bool operator >(Decimal38 left, Decimal38 right)
    {
        return left.Value > right.Value;
    }

    public static bool operator >=(Decimal38 left, Decimal38 right)
    {
        return left.Value >= right.Value;
    }

    public static bool operator <(Decimal38 left, Decimal38 right)
    {
        return left.Value < right.Value;
    }

    public static bool operator <=(Decimal38 left, Decimal38 right)
    {
        return left.Value <= right.Value;
    }

    public static Decimal38 operator --(Decimal38 value)
    {
        return value - One;
    }

    public static Decimal38 operator *(Decimal38 left, Decimal38 right)
    {
        return FromScaled(left.Value / half_factor * right.Value / half_factor);
    }

    public static Decimal38 operator /(Decimal38 left, Decimal38 right)
    {
        return FromScaled(left.Value * factor / right.Value);
    }

    public static Decimal38 operator ++(Decimal38 low)
    {
        return low + One;
    }

    public static Decimal38 operator %(Decimal38 left, Decimal38 right)
    {
        throw new NotImplementedException();
    }

    public int CompareTo(object? obj)
    {
        if (obj is Decimal38 other)
        {
            return CompareTo(other);
        }
        return 1;
    }

    public int CompareTo(Decimal38 other)
    {
        return Value.CompareTo(other.Value);
    }

    public bool Equals(Decimal38 other)
    {
        return Value == other.Value;
    }

    public string ToString(string? format, IFormatProvider? formatProvider)
    {
        return AsDecimal().ToString(format, formatProvider);
    }

    public static Decimal38 Max(Decimal38 x, Decimal38 y)
    {
        return x > y ? x : y;
    }

    public static Decimal38 Min(Decimal38 x, Decimal38 y)
    {
        return x < y ? x : y;
    }

    public bool TryFormat(Span<char> destination, out int charsWritten, ReadOnlySpan<char> format, IFormatProvider? provider)
    {
        return AsDecimal().TryFormat(destination, out charsWritten, format, provider);
    }

    public static Decimal38 Parse(string s, IFormatProvider? provider)
    {
        return new Decimal38(double.Parse(s, provider));
    }

    public static bool TryParse([NotNullWhen(true)] string? s, IFormatProvider? provider, out Decimal38 result)
    {
        var success = double.TryParse(s, NumberStyles.Any, provider, out var d);
        if (success)
        {
            result = new Decimal38(d);
            return true;
        }
        result = default;
        return false;
    }

    public static Decimal38 Parse(ReadOnlySpan<char> s, IFormatProvider? provider)
    {
        throw new NotImplementedException();
    }

    public static bool TryParse(ReadOnlySpan<char> s, IFormatProvider? provider, out Decimal38 result)
    {
        throw new NotImplementedException();
    }

    public static Decimal38 AdditiveIdentity { get; }

    public static Decimal38 MultiplicativeIdentity { get; }

    public static bool IsCanonical(Decimal38 low)
    {
        throw new NotImplementedException();
    }

    public static bool IsComplexNumber(Decimal38 low)
    {
        throw new NotImplementedException();
    }

    public static bool IsEvenInteger(Decimal38 low)
    {
        throw new NotImplementedException();
    }

    public static bool IsFinite(Decimal38 low)
    {
        throw new NotImplementedException();
    }

    public static bool IsImaginaryNumber(Decimal38 low)
    {
        throw new NotImplementedException();
    }

    public static bool IsInfinity(Decimal38 low)
    {
        throw new NotImplementedException();
    }

    public static bool IsInteger(Decimal38 low)
    {
        throw new NotImplementedException();
    }

    public static bool IsNaN(Decimal38 low)
    {
        return false;
    }

    public static bool IsNegative(Decimal38 low)
    {
        return low.Value < 0;
    }

    public static bool IsNegativeInfinity(Decimal38 low)
    {
        throw new NotImplementedException();
    }

    public static bool IsNormal(Decimal38 low)
    {
        throw new NotImplementedException();
    }

    public static bool IsOddInteger(Decimal38 low)
    {
        throw new NotImplementedException();
    }

    public static bool IsPositive(Decimal38 low)
    {
        throw new NotImplementedException();
    }

    public static bool IsPositiveInfinity(Decimal38 low)
    {
        throw new NotImplementedException();
    }

    public static bool IsRealNumber(Decimal38 low)
    {
        throw new NotImplementedException();
    }

    public static bool IsSubnormal(Decimal38 low)
    {
        throw new NotImplementedException();
    }

    public static bool IsZero(Decimal38 low)
    {
        throw new NotImplementedException();
    }

    public static Decimal38 MaxMagnitude(Decimal38 x, Decimal38 y)
    {
        throw new NotImplementedException();
    }

    public static Decimal38 MaxMagnitudeNumber(Decimal38 x, Decimal38 y)
    {
        throw new NotImplementedException();
    }

    public static Decimal38 MinMagnitude(Decimal38 x, Decimal38 y)
    {
        throw new NotImplementedException();
    }

    public static Decimal38 MinMagnitudeNumber(Decimal38 x, Decimal38 y)
    {
        throw new NotImplementedException();
    }

    public static Decimal38 Parse(ReadOnlySpan<char> s, NumberStyles style, IFormatProvider? provider)
    {
        throw new NotImplementedException();
    }

    public static Decimal38 Parse(string s, NumberStyles style, IFormatProvider? provider)
    {
        throw new NotImplementedException();
    }

    public static bool TryConvertFromChecked<TOther>(TOther Low, out Decimal38 result) where TOther : INumberBase<TOther>
    {
        throw new NotImplementedException();
    }

    public static bool TryConvertFromSaturating<TOther>(TOther Low, out Decimal38 result) where TOther : INumberBase<TOther>
    {
        throw new NotImplementedException();
    }

    public static bool TryConvertFromTruncating<TOther>(TOther Low, out Decimal38 result) where TOther : INumberBase<TOther>
    {
        throw new NotImplementedException();
    }

    public static bool TryConvertToChecked<TOther>(Decimal38 low, [MaybeNullWhen(false)] out TOther result) where TOther : INumberBase<TOther>
    {
        throw new NotImplementedException();
    }

    public static bool TryConvertToSaturating<TOther>(Decimal38 low, [MaybeNullWhen(false)] out TOther result) where TOther : INumberBase<TOther>
    {
        throw new NotImplementedException();
    }

    public static bool TryConvertToTruncating<TOther>(Decimal38 low, [MaybeNullWhen(false)] out TOther result) where TOther : INumberBase<TOther>
    {
        throw new NotImplementedException();
    }

    public static bool TryParse(ReadOnlySpan<char> s, NumberStyles style, IFormatProvider? provider, out Decimal38 result)
    {
        throw new NotImplementedException();
    }

    public static bool TryParse([NotNullWhen(true)] string? s, NumberStyles style, IFormatProvider? provider, out Decimal38 result)
    {
        throw new NotImplementedException();
    }

    public static implicit operator Decimal38(int i) => new Decimal38(i);
    public static explicit operator int(Decimal38 d) => (int)(d.Value / factor);

    public static implicit operator Decimal38(decimal d) => new Decimal38(d);
    public static explicit operator decimal(Decimal38 d) => d.AsDecimal();

    public TypeCode GetTypeCode()
    {
        throw new NotImplementedException();
    }

    public bool ToBoolean(IFormatProvider? provider)
    {
        throw new NotImplementedException();
    }

    public byte ToByte(IFormatProvider? provider)
    {
        throw new NotImplementedException();
    }

    public char ToChar(IFormatProvider? provider)
    {
        throw new NotImplementedException();
    }

    public DateTime ToDateTime(IFormatProvider? provider)
    {
        throw new NotImplementedException();
    }

    public decimal ToDecimal(IFormatProvider? provider)
    {
        throw new NotImplementedException();
    }

    public double ToDouble(IFormatProvider? provider)
    {
        return (double)Value / factor;
    }

    public short ToInt16(IFormatProvider? provider)
    {
        throw new NotImplementedException();
    }

    public int ToInt32(IFormatProvider? provider)
    {
        return (int)(Value / factor);
    }

    public long ToInt64(IFormatProvider? provider)
    {
        return (long)Value / factor;
    }

    public sbyte ToSByte(IFormatProvider? provider)
    {
        throw new NotImplementedException();
    }

    public float ToSingle(IFormatProvider? provider)
    {
        throw new NotImplementedException();
    }

    public string ToString(IFormatProvider? provider)
    {
        return AsDecimal().ToString(provider);
    }

    public object ToType(Type conversionType, IFormatProvider? provider)
    {
        throw new NotImplementedException();
    }

    public ushort ToUInt16(IFormatProvider? provider)
    {
        throw new NotImplementedException();
    }

    public uint ToUInt32(IFormatProvider? provider)
    {
        throw new NotImplementedException();
    }

    public ulong ToUInt64(IFormatProvider? provider)
    {
        throw new NotImplementedException();
    }

    public static Decimal38 MaxValue => FromScaled(Int128.MaxValue);
    public static Decimal38 MinValue => FromScaled(Int128.MinValue);
}
