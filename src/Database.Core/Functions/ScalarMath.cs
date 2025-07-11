using System.Numerics;

namespace Database.Core.Functions;

public interface IScalerFunction
{
}

public interface ScalarMathOneLeft<T> : IScalerFunction
    where T : INumber<T>
{
    public int LeftIndex { get; }

    public T Value { get; }

    T[] Execute(T[] left);
}

public interface ScalarMathOneRight<T> : IScalerFunction
    where T : INumber<T>
{
    public T Value { get; }

    public int RightIndex { get; }

    T[] Execute(T[] right);
}

public interface ScalarMathTwo<T> : IScalerFunction
    where T : INumber<T>
{
    public int LeftIndex { get; }

    public int RightIndex { get; }

    T[] Execute(T[] left, T[] right);
}

public record SumOneLeft<T>(int LeftIndex, T Value) : ScalarMathOneLeft<T>
    where T : INumber<T>
{
    public T[] Execute(T[] values)
    {
        var result = new T[values.Length];
        for (var i = 0; i < values.Length; i++)
        {
            result[i] = values[i] + Value;
        }

        return result;
    }
}

public record SumOneRight<T>(int RightIndex, T Value) : ScalarMathOneRight<T>
    where T : INumber<T>
{
    public T[] Execute(T[] values)
    {
        var result = new T[values.Length];
        for (var i = 0; i < values.Length; i++)
        {
            result[i] = Value + values[i];
        }

        return result;
    }
}

public record SumTwo<T>(int LeftIndex, int RightIndex) : ScalarMathTwo<T>
    where T : INumber<T>
{
    public T[] Execute(T[] left, T[] right)
    {
        var result = new T[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] + right[i];
        }

        return result;
    }
}

