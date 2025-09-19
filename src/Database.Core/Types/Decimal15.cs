using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Numerics;

namespace Database.Core.Types;

[DebuggerDisplay("{AsDecimal()}")]
public readonly struct Decimal15 : INumber<Decimal15>
{
    public Decimal15(float value)
    {
        Value = (long)(value * factor);
    }

    public Decimal15(double value)
    {
        Value = (long)(value * factor);
    }

    public Decimal15(decimal value)
    {
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
        throw new NotImplementedException();
    }

    public int CompareTo(Decimal15 other)
    {
        throw new NotImplementedException();
    }

    public bool Equals(Decimal15 other)
    {
        throw new NotImplementedException();
    }

    public string ToString(string? format, IFormatProvider? formatProvider)
    {
        throw new NotImplementedException();
    }

    public bool TryFormat(Span<char> destination, out int charsWritten, ReadOnlySpan<char> format, IFormatProvider? provider)
    {
        throw new NotImplementedException();
    }

    public static Decimal15 Parse(string s, IFormatProvider? provider)
    {
        throw new NotImplementedException();
    }

    public static bool TryParse([NotNullWhen(true)] string? s, IFormatProvider? provider, out Decimal15 result)
    {
        throw new NotImplementedException();
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
        throw new NotImplementedException();
    }

    public static bool IsNegative(Decimal15 value)
    {
        throw new NotImplementedException();
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
}
