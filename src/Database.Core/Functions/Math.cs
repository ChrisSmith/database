using System.Numerics;
using Database.Core.Catalog;

namespace Database.Core.Functions;

public interface IAggregateFunction : IFunction
{
    object? GetValue();
}
/**
 * Operator on the entire column, one row at a time
 */
public interface IAggregateFunction<In, Out> : IAggregateFunction
{
    void Next(In[] value);

    Out Value();
}

public record Count<In> : IAggregateFunction<In, int>
    where In : INumber<In>
{
    public DataType ReturnType => DataType.Int;

    private int _state = 0;
    public int Value() => _state;
    public object? GetValue() => Value();

    public void Next(In[] value)
    {
        _state += value.Length;
    }
}

public record StringCount : IAggregateFunction<string?, int>
{
    public DataType ReturnType => DataType.String;

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

public record Sum<T>(DataType ReturnType) : IAggregateFunction<T, T>
    where T : INumber<T>
{
    private T _state = default!;
    public T Value() => _state;
    public object? GetValue() => Value();

    public void Next(T[] value)
    {
        foreach (var item in value)
        {
            _state += item;
        }
    }
}

public record Avg<T> : IAggregateFunction<T, double>
    where T : INumber<T>
{
    public DataType ReturnType => DataType.Double;

    private T _sum = default!;
    private int _count = 0;
    public double Value() => (double)Convert.ChangeType(_sum, typeof(double)) / _count;
    public object? GetValue() => Value();

    public void Next(T[] value)
    {
        foreach (var item in value)
        {
            _sum += item;
        }
        _count += value.Length;
    }
}
