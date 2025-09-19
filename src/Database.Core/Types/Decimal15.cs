using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Numerics;
using System.Reflection;
using System.Reflection.Emit;

namespace Database.Core.Types;

[DebuggerDisplay("{AsDecimal()}")]
public readonly struct Decimal15 : INumber<Decimal15>, IMinMaxValue<Decimal15>, IConvertible
{
    public Decimal15(float value)
    {
        Value = (long)(value * factor);
    }

    public Decimal15(double value)
    {
        Value = (long)(value * factor);
    }

    private static readonly Func<decimal, ulong> _low64;
    static Decimal15()
    {
        var field = typeof(decimal)
            .GetField("_lo64", BindingFlags.NonPublic | BindingFlags.Instance)!;

        var method = new DynamicMethod("GetDecimalLo", typeof(ulong), new[] { typeof(decimal) }, typeof(decimal), true);
        var il = method.GetILGenerator();
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, field);
        il.Emit(OpCodes.Ret);

        _low64 = (Func<decimal, ulong>)method.CreateDelegate(typeof(Func<decimal, ulong>));
    }

    public Decimal15(decimal value)
    {
        if (value.Scale == 2)
        {
            var v = _low64(value);
            Value = (long)v;
            return;
        }

        Value = (long)(value * factor);
    }

    public Decimal15(int value)
    {
        Value = value * factor;
    }

    public Decimal15(long value)
    {
        Value = value * factor;
    }

    public static Decimal15 FromScaled(long value)
    {
        return new Decimal15
        {
            Value = value,
        };
    }

    public long Value { get; private init; }

    public decimal AsDecimal() => new decimal(Math.Round(Value / (float)factor, 2));

    private const int scale = 2;
    private const int factor = 100; // 10^scale

    public static Decimal15 One => new(1);
    public static int Radix => 2;
    public static Decimal15 Zero => new(0);

    public static Decimal15 operator +(Decimal15 left, Decimal15 right)
    {
        return FromScaled(left.Value + right.Value);
    }

    public static Decimal15 operator -(Decimal15 left, Decimal15 right)
    {
        return FromScaled(left.Value - right.Value);
    }

    public static Decimal15 operator -(Decimal15 value)
    {
        return FromScaled(-value.Value);
    }

    public static Decimal15 operator +(Decimal15 value)
    {
        return value;
    }

    public static Decimal15 Abs(Decimal15 value)
    {
        return FromScaled(Math.Abs(value.Value));
    }

    public static bool operator ==(Decimal15 left, Decimal15 right)
    {
        return left.Value == right.Value;
    }

    public static bool operator !=(Decimal15 left, Decimal15 right)
    {
        return left.Value != right.Value;
    }

    public static bool operator >(Decimal15 left, Decimal15 right)
    {
        return left.Value > right.Value;
    }

    public static bool operator >=(Decimal15 left, Decimal15 right)
    {
        return left.Value >= right.Value;
    }

    public static bool operator <(Decimal15 left, Decimal15 right)
    {
        return left.Value < right.Value;
    }

    public static bool operator <=(Decimal15 left, Decimal15 right)
    {
        return left.Value <= right.Value;
    }

    public static Decimal15 operator --(Decimal15 value)
    {
        return FromScaled(value.Value - factor);
    }

    public static Decimal15 operator *(Decimal15 left, Decimal15 right)
    {
        return FromScaled(left.Value * right.Value / factor);
    }

    public static Decimal15 operator /(Decimal15 left, Decimal15 right)
    {
        return FromScaled(left.Value * factor / right.Value);
    }

    public static Decimal15 operator ++(Decimal15 value)
    {
        return FromScaled(value.Value + factor);
    }

    public static Decimal15 operator %(Decimal15 left, Decimal15 right)
    {
        return FromScaled(left.Value % right.Value);
    }

    public int CompareTo(object? obj)
    {
        if (obj is Decimal15 other)
        {
            return CompareTo(other);
        }
        return 1;
    }

    public int CompareTo(Decimal15 other)
    {
        return Value.CompareTo(other.Value);
    }

    public bool Equals(Decimal15 other)
    {
        return Value == other.Value;
    }

    public string ToString(string? format, IFormatProvider? formatProvider)
    {
        return AsDecimal().ToString(format, formatProvider);
    }

    public static Decimal15 Max(Decimal15 x, Decimal15 y)
    {
        return x > y ? x : y;
    }

    public static Decimal15 Min(Decimal15 x, Decimal15 y)
    {
        return x < y ? x : y;
    }

    public bool TryFormat(Span<char> destination, out int charsWritten, ReadOnlySpan<char> format, IFormatProvider? provider)
    {
        return AsDecimal().TryFormat(destination, out charsWritten, format, provider);
    }

    public static Decimal15 Parse(string s, IFormatProvider? provider)
    {
        return new Decimal15(double.Parse(s, provider));
    }

    public static bool TryParse([NotNullWhen(true)] string? s, IFormatProvider? provider, out Decimal15 result)
    {
        var success = double.TryParse(s, NumberStyles.Any, provider, out var d);
        if (success)
        {
            result = new Decimal15(d);
            return true;
        }
        result = default;
        return false;
    }

    public static Decimal15 Parse(ReadOnlySpan<char> s, IFormatProvider? provider)
    {
        throw new NotImplementedException();
    }

    public static bool TryParse(ReadOnlySpan<char> s, IFormatProvider? provider, out Decimal15 result)
    {
        throw new NotImplementedException();
    }

    public static Decimal15 AdditiveIdentity { get; }

    public static Decimal15 MultiplicativeIdentity { get; }

    public static bool IsCanonical(Decimal15 value)
    {
        throw new NotImplementedException();
    }

    public static bool IsComplexNumber(Decimal15 value)
    {
        throw new NotImplementedException();
    }

    public static bool IsEvenInteger(Decimal15 value)
    {
        throw new NotImplementedException();
    }

    public static bool IsFinite(Decimal15 value)
    {
        throw new NotImplementedException();
    }

    public static bool IsImaginaryNumber(Decimal15 value)
    {
        throw new NotImplementedException();
    }

    public static bool IsInfinity(Decimal15 value)
    {
        throw new NotImplementedException();
    }

    public static bool IsInteger(Decimal15 value)
    {
        throw new NotImplementedException();
    }

    public static bool IsNaN(Decimal15 value)
    {
        return false;
    }

    public static bool IsNegative(Decimal15 value)
    {
        return value.Value < 0;
    }

    public static bool IsNegativeInfinity(Decimal15 value)
    {
        throw new NotImplementedException();
    }

    public static bool IsNormal(Decimal15 value)
    {
        throw new NotImplementedException();
    }

    public static bool IsOddInteger(Decimal15 value)
    {
        throw new NotImplementedException();
    }

    public static bool IsPositive(Decimal15 value)
    {
        throw new NotImplementedException();
    }

    public static bool IsPositiveInfinity(Decimal15 value)
    {
        throw new NotImplementedException();
    }

    public static bool IsRealNumber(Decimal15 value)
    {
        throw new NotImplementedException();
    }

    public static bool IsSubnormal(Decimal15 value)
    {
        throw new NotImplementedException();
    }

    public static bool IsZero(Decimal15 value)
    {
        throw new NotImplementedException();
    }

    public static Decimal15 MaxMagnitude(Decimal15 x, Decimal15 y)
    {
        throw new NotImplementedException();
    }

    public static Decimal15 MaxMagnitudeNumber(Decimal15 x, Decimal15 y)
    {
        throw new NotImplementedException();
    }

    public static Decimal15 MinMagnitude(Decimal15 x, Decimal15 y)
    {
        throw new NotImplementedException();
    }

    public static Decimal15 MinMagnitudeNumber(Decimal15 x, Decimal15 y)
    {
        throw new NotImplementedException();
    }

    public static Decimal15 Parse(ReadOnlySpan<char> s, NumberStyles style, IFormatProvider? provider)
    {
        throw new NotImplementedException();
    }

    public static Decimal15 Parse(string s, NumberStyles style, IFormatProvider? provider)
    {
        throw new NotImplementedException();
    }

    public static bool TryConvertFromChecked<TOther>(TOther value, out Decimal15 result) where TOther : INumberBase<TOther>
    {
        throw new NotImplementedException();
    }

    public static bool TryConvertFromSaturating<TOther>(TOther value, out Decimal15 result) where TOther : INumberBase<TOther>
    {
        throw new NotImplementedException();
    }

    public static bool TryConvertFromTruncating<TOther>(TOther value, out Decimal15 result) where TOther : INumberBase<TOther>
    {
        throw new NotImplementedException();
    }

    public static bool TryConvertToChecked<TOther>(Decimal15 value, [MaybeNullWhen(false)] out TOther result) where TOther : INumberBase<TOther>
    {
        throw new NotImplementedException();
    }

    public static bool TryConvertToSaturating<TOther>(Decimal15 value, [MaybeNullWhen(false)] out TOther result) where TOther : INumberBase<TOther>
    {
        throw new NotImplementedException();
    }

    public static bool TryConvertToTruncating<TOther>(Decimal15 value, [MaybeNullWhen(false)] out TOther result) where TOther : INumberBase<TOther>
    {
        throw new NotImplementedException();
    }

    public static bool TryParse(ReadOnlySpan<char> s, NumberStyles style, IFormatProvider? provider, out Decimal15 result)
    {
        throw new NotImplementedException();
    }

    public static bool TryParse([NotNullWhen(true)] string? s, NumberStyles style, IFormatProvider? provider, out Decimal15 result)
    {
        throw new NotImplementedException();
    }

    public static implicit operator Decimal15(int i) => new Decimal15(i);
    public static explicit operator int(Decimal15 d) => (int)(d.Value / factor);

    public static implicit operator Decimal15(decimal d) => new Decimal15(d);
    public static explicit operator decimal(Decimal15 d) => d.AsDecimal();

    public static Decimal15 MaxValue { get; } = FromScaled(long.MaxValue);
    public static Decimal15 MinValue { get; } = FromScaled(long.MinValue);
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
        return (double)AsDecimal();
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
        return Value / factor;
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
}
