using System.Numerics;
using Database.Core.Catalog;
using Database.Core.Types;

namespace Database.Core.Functions;

public interface IScalarMathOne<TIn, TOut> : IFunction
{
    TOut[] Execute(TIn[] values);
}

public abstract record Cast<TIn, TOut>(DataType ReturnType) : IScalarMathOne<TIn, TOut>
{
    public abstract TOut[] Execute(TIn[] values);
}

public record CastInt<T>() : Cast<T, int>(DataType.Int)
{
    public override int[] Execute(T[] values)
    {
        var result = new int[values.Length];
        for (var i = 0; i < values.Length; i++)
        {
            result[i] = Convert.ToInt32(values[i]);
        }
        return result;
    }
}

public record CastLong<T>() : Cast<T, long>(DataType.Long)
{
    public override long[] Execute(T[] values)
    {
        var result = new long[values.Length];
        for (var i = 0; i < values.Length; i++)
        {
            result[i] = Convert.ToInt64(values[i]);
        }
        return result;
    }
}

public record CastFloat<T>() : Cast<T, float>(DataType.Float)
{
    public override float[] Execute(T[] values)
    {
        var result = new float[values.Length];
        for (var i = 0; i < values.Length; i++)
        {
            result[i] = Convert.ToSingle(values[i]);
        }

        return result;
    }
}

public record CastDouble<T>() : Cast<T, double>(DataType.Double)
{
    public override double[] Execute(T[] values)
    {
        var result = new double[values.Length];
        for (var i = 0; i < values.Length; i++)
        {
            result[i] = Convert.ToDouble(values[i]);
        }
        return result;
    }
}

public record CastDecimal<T>() : Cast<T, Decimal15>(DataType.Decimal15)
{
    public override Decimal15[] Execute(T[] values)
    {
        var result = new Decimal15[values.Length];
        for (var i = 0; i < values.Length; i++)
        {
            result[i] = Convert.ToDecimal(values[i]);
        }
        return result;
    }
}

public record CastDateToDateTime() : Cast<DateOnly, DateTime>(DataType.DateTime)
{
    public override DateTime[] Execute(DateOnly[] values)
    {
        var result = new DateTime[values.Length];
        for (var i = 0; i < values.Length; i++)
        {
            result[i] = values[i].ToDateTime(TimeOnly.MinValue);
        }
        return result;
    }
}
