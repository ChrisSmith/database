using System.Numerics;
using Database.Core.Catalog;

namespace Database.Core.Functions;

public interface IFunction
{
    public DataType ReturnType { get; }
}

// Left refers to the left column, so multiply one left is left_col * const value

public interface ScalarMathOneLeft<T> : IFunction
    where T : INumber<T>
{
    public int LeftIndex { get; }

    public T Value { get; }

    T[] Execute(T[] left);
}

public interface ScalarMathOneRight<T> : IFunction
    where T : INumber<T>
{
    public T Value { get; }

    public int RightIndex { get; }

    T[] Execute(T[] right);
}

public interface ScalarMathTwo<T> : IFunction
    where T : INumber<T>
{
    public int LeftIndex { get; }

    public int RightIndex { get; }

    T[] Execute(T[] left, T[] right);
}

public record SumOne<T>(int LeftIndex, T Value, DataType ReturnType) : ScalarMathOneLeft<T>
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

public record SumTwo<T>(int LeftIndex, int RightIndex, DataType ReturnType) : ScalarMathTwo<T>
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

public record MultiplyTwo<T>(int LeftIndex, int RightIndex, DataType ReturnType) : ScalarMathTwo<T>
    where T : INumber<T>
{
    public T[] Execute(T[] left, T[] right)
    {
        var result = new T[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] * right[i];
        }
        return result;
    }
}

public record MultiplyOne<T>(int LeftIndex, T Value, DataType ReturnType) : ScalarMathOneLeft<T>
    where T : INumber<T>
{
    public T[] Execute(T[] values)
    {
        var result = new T[values.Length];
        for (var i = 0; i < values.Length; i++)
        {
            result[i] = values[i] * Value;
        }
        return result;
    }
}

public record MinusOneRight<T>(int RightIndex, T Value, DataType ReturnType) : ScalarMathOneRight<T>
    where T : INumber<T>
{
    public T[] Execute(T[] values)
    {
        var result = new T[values.Length];
        for (var i = 0; i < values.Length; i++)
        {
            result[i] = Value - values[i];
        }
        return result;
    }
}

public record MinusOneLeft<T>(int LeftIndex, T Value, DataType ReturnType) : ScalarMathOneLeft<T>
    where T : INumber<T>
{
    public T[] Execute(T[] values)
    {
        var result = new T[values.Length];
        for (var i = 0; i < values.Length; i++)
        {
            result[i] = values[i] - Value;
        }
        return result;
    }
}

public record MinusTwo<T>(int LeftIndex, int RightIndex, DataType ReturnType) : ScalarMathTwo<T>
    where T : INumber<T>
{
    public T[] Execute(T[] left, T[] right)
    {
        var result = new T[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] - right[i];
        }
        return result;
    }
}

public record DivideOneRight<T>(int RightIndex, T Value, DataType ReturnType) : ScalarMathOneRight<T>
    where T : INumber<T>
{
    public T[] Execute(T[] values)
    {
        var result = new T[values.Length];
        for (var i = 0; i < values.Length; i++)
        {
            result[i] = Value / values[i];
        }
        return result;
    }
}

public record DivideOneLeft<T>(int LeftIndex, T Value, DataType ReturnType) : ScalarMathOneLeft<T>
    where T : INumber<T>
{
    public T[] Execute(T[] values)
    {
        var result = new T[values.Length];
        for (var i = 0; i < values.Length; i++)
        {
            result[i] = values[i] / Value;
        }
        return result;
    }
}

public record DivideTwo<T>(int LeftIndex, int RightIndex, DataType ReturnType) : ScalarMathTwo<T>
    where T : INumber<T>
{
    public T[] Execute(T[] left, T[] right)
    {
        var result = new T[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] / right[i];
        }
        return result;
    }
}

public record ModuloOneRight<T>(int RightIndex, T Value, DataType ReturnType) : ScalarMathOneRight<T>
    where T : INumber<T>
{
    public T[] Execute(T[] values)
    {
        var result = new T[values.Length];
        for (var i = 0; i < values.Length; i++)
        {
            result[i] = Value % values[i];
        }
        return result;
    }
}

public record ModuloOneLeft<T>(int LeftIndex, T Value, DataType ReturnType) : ScalarMathOneLeft<T>
    where T : INumber<T>
{
    public T[] Execute(T[] values)
    {
        var result = new T[values.Length];
        for (var i = 0; i < values.Length; i++)
        {
            result[i] = values[i] % Value;
        }
        return result;
    }
}

public record ModuloTwo<T>(int LeftIndex, int RightIndex, DataType ReturnType) : ScalarMathTwo<T>
    where T : INumber<T>
{
    public T[] Execute(T[] left, T[] right)
    {
        var result = new T[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] % right[i];
        }
        return result;
    }
}
