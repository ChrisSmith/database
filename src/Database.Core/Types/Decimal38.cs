namespace Database.Core.Types;

public readonly struct Decimal38(long high, long low, byte scale)
{
    public long High { get; } = high;
    public long Low { get; } = low;
    public byte Scale { get; } = scale;

    public static Decimal38 operator +(Decimal38 left, Decimal38 right)
    {
        return new Decimal38(left.High + right.High, left.Low + right.Low, left.Scale);
    }
}
