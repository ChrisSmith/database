using System.Numerics;
using Database.Core.Catalog;

namespace Database.Core.Functions;

public abstract record BoolFunction : IFunction
{
    public DataType ReturnType => DataType.Bool;
}

public interface IFilterFunctionOne<In> : IFunction
{
    public bool[] Ok(In[] values);
}

public interface IFilterFunctionTwo<In> : IFunction
{
    public bool[] Ok(In[] left, In[] right);
}

public interface IFilterThreeColsThree<In> : IFunction
{
    public bool[] Ok(In[] value, In[] lower, In[] upper);
}

# region Greater Than
public record GreaterThanTwo<T>() : BoolFunction, IFilterFunctionTwo<T>
    where T : INumber<T>
{
    public bool[] Ok(T[] left, T[] right)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] > right[i];
        }
        return result;
    }
}

public record GreaterThanEqualTwo<T>() : BoolFunction, IFilterFunctionTwo<T>
    where T : INumber<T>
{
    public bool[] Ok(T[] left, T[] right)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] >= right[i];
        }
        return result;
    }
}

#endregion

# region Less Than
public record LessThanTwo<T>() : BoolFunction, IFilterFunctionTwo<T>
    where T : INumber<T>
{
    public bool[] Ok(T[] left, T[] right)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] < right[i];
        }
        return result;
    }
}

public record LessThanEqualTwo<T>() : BoolFunction, IFilterFunctionTwo<T>
    where T : INumber<T>
{
    public bool[] Ok(T[] left, T[] right)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] <= right[i];
        }
        return result;
    }
}

#endregion

#region Equal

public record EqualTwo<T>() : BoolFunction(), IFilterFunctionTwo<T>
    where T : INumber<T>
{
    public bool[] Ok(T[] left, T[] right)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] == right[i];
        }
        return result;
    }
}

public record EqualTwoString() : BoolFunction(), IFilterFunctionTwo<string>
{
    public bool[] Ok(string[] left, string[] right)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] == right[i];
        }
        return result;
    }
}

public record NotEqualTwoString() : BoolFunction(), IFilterFunctionTwo<string>
{
    public bool[] Ok(string[] left, string[] right)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] != right[i];
        }
        return result;
    }
}

public record NotEqualTwo<T> : BoolFunction, IFilterFunctionTwo<T>
    where T : INumber<T>
{
    public bool[] Ok(T[] left, T[] right)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] != right[i];
        }
        return result;
    }
}

#endregion

#region Between

public record Between<T>() : BoolFunction, IFilterThreeColsThree<T>
    where T : INumber<T>
{
    public bool[] Ok(T[] value, T[] lower, T[] upper)
    {
        var result = new bool[value.Length];
        for (var i = 0; i < value.Length; i++)
        {
            result[i] = lower[i] <= value[i] && value[i] <= upper[i];
        }
        return result;
    }
}

public record BetweenDateTime() : BoolFunction, IFilterThreeColsThree<DateTime>
{
    public bool[] Ok(DateTime[] value, DateTime[] lower, DateTime[] upper)
    {
        var result = new bool[value.Length];
        for (var i = 0; i < value.Length; i++)
        {
            result[i] = lower[i] <= value[i] && value[i] <= upper[i];
        }
        return result;
    }
}

#endregion
