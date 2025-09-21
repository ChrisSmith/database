using System.Numerics;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Types;

namespace Database.Core.Functions;

public interface IFunction
{
    public DataType ReturnType { get; }
}

public interface IFunctionWithRowGroup : IFunction
{
    public IColumn Execute(RowGroup rowGroup, CancellationToken token);
}

public interface IFunctionWithColumnLength : IFunction
{
    IColumn Execute(int length);
}

public interface IScalarMathTwo<T> : IFunction
    where T : INumber<T>
{
    T[] Execute(T[] left, T[] right);
}

public interface IScalarMathTwoFull<TIn, TOut> : IFunction
    where TIn : INumber<TIn>
    where TOut : INumber<TOut>
{
    TOut[] Execute(TIn[] left, TIn[] right);
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

public record MultiplyTwoDecimals : IScalarMathTwoFull<Decimal15, Decimal38>
{
    public DataType ReturnType => DataType.Decimal38;

    public Decimal38[] Execute(Decimal15[] left, Decimal15[] right)
    {
        var result = new Decimal38[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = new Decimal38(left[i]) * new Decimal38(right[i]);
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

public record DivideTwoDecimals : IScalarMathTwoFull<Decimal15, Decimal38>
{
    public DataType ReturnType => DataType.Decimal38;

    public Decimal38[] Execute(Decimal15[] left, Decimal15[] right)
    {
        var result = new Decimal38[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = new Decimal38(left[i]) / new Decimal38(right[i]);
        }
        return result;
    }
}

public record DivideTwoDecimals38 : IScalarMathTwoFull<Decimal38, double>
{
    public DataType ReturnType => DataType.Double;

    public double[] Execute(Decimal38[] left, Decimal38[] right)
    {
        var result = new double[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i].ToDouble(null) / right[i].ToDouble(null);
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
