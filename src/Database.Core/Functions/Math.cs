namespace Database.Core.Functions;

// TODO pure vs non-pure functions
// Some can use an accumulator for state, some cannot?

public interface AggregateValue
{
    int ColumnIndex { get; }

    object? GetValue();
}

/**
 * Operator on the entire column, one row at a time
 */
public interface AggregateValue<In, Out> : AggregateValue
{
    void Next(In[] value);

    Out Value();
}

// TODO checkout using System.Numerics
// https://learn.microsoft.com/en-us/dotnet/standard/generics/math
public record DoubleCount(int ColumnIndex) : AggregateValue<double?, int>
{
    private int _state = 0;
    public int Value() => _state;
    public object? GetValue() => Value();

    public void Next(double?[] value)
    {
        foreach (var item in value)
        {
            if (item.HasValue)
            {
                _state += 1;
            }
        }
    }
}

public record NullableIntCount(int ColumnIndex) : AggregateValue<int?, int>
{
    private int _state = 0;
    public int Value() => _state;
    public object? GetValue() => Value();

    public void Next(int?[] value)
    {
        foreach (var item in value)
        {
            if (item.HasValue)
            {
                _state += 1;
            }
        }
    }
}

public record IntCount(int ColumnIndex) : AggregateValue<int, int>
{
    private int _state = 0;
    public int Value() => _state;
    public object? GetValue() => Value();

    public void Next(int[] value)
    {
        _state += value.Length;
    }
}

public record StringCount(int ColumnIndex) : AggregateValue<string?, int>
{
    private int _state = 0;
    public int Value() => _state;
    public object? GetValue() => Value();

    public void Next(string?[] value)
    {
        foreach (var item in value)
        {
            if (item != null)
            {
                _state += 1;
            }
        }
    }
}

public record IntSum(int ColumnIndex) : AggregateValue<int, int>
{
    private int _state = 0;
    public int Value() => _state;
    public object? GetValue() => Value();

    public void Next(int[] value)
    {
        foreach (var item in value)
        {
            _state += item;
        }
    }
}

public record DoubleSum(int ColumnIndex) : AggregateValue<double, double>
{
    private double _state = 0;
    public double Value() => _state;
    public object? GetValue() => Value();

    public void Next(double[] value)
    {
        foreach (var item in value)
        {
            _state += item;
        }
    }
}

public record IntAvg(int ColumnIndex) : AggregateValue<int, double>
{
    private int _sum = 0;
    private int _count = 0;
    public double Value() => (double)_sum / _count;
    public object? GetValue() => Value();

    public void Next(int[] value)
    {
        foreach (var item in value)
        {
            _sum += item;
        }
        _count += value.Length;
    }
}

public record DoubleAvg(int ColumnIndex) : AggregateValue<double, double>
{
    private double _sum = 0;
    private int _count = 0;
    public double Value() => _sum / _count;
    public object? GetValue() => Value();

    public void Next(double[] value)
    {
        foreach (var item in value)
        {
            _sum += item;
        }
        _count += value.Length;
    }
}
