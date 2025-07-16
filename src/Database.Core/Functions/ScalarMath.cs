using System.Numerics;
using Database.Core.Catalog;

namespace Database.Core.Functions;

public interface IFunction
{
    public DataType ReturnType { get; }
}

public interface IScalarMathTwo<T> : IFunction
    where T : INumber<T>
{
    T[] Execute(T[] left, T[] right);
}

public record SumTwo<T>(DataType ReturnType) : IScalarMathTwo<T>
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

public record MultiplyTwo<T>(DataType ReturnType) : IScalarMathTwo<T>
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

public record MinusTwo<T>(DataType ReturnType) : IScalarMathTwo<T>
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

public record DivideTwo<T>(DataType ReturnType) : IScalarMathTwo<T>
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

public record ModuloTwo<T>(DataType ReturnType) : IScalarMathTwo<T>
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
