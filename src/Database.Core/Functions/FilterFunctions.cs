using System.Numerics;
using Database.Core.Catalog;

namespace Database.Core.Functions;

public interface IFilterFunctionOne<In> : IFunction
{
    public int LeftIndex { get; }

    public bool[] Ok(In[] left);
}

public abstract record BoolFunction : IFunction
{
    public DataType ReturnType => DataType.Bool;
}

public interface IFilterFunctionTwo<In> : IFunction
{
    public int LeftIndex { get; }

    public int RightIndex { get; }

    public bool[] Ok(In[] left, In[] right);
}

public interface IFilterThreeColsThree<In> : IFunction
{
    public int LeftIndex { get; }

    public int ValueIndex { get; }

    public int RightIndex { get; }

    public bool[] Ok(In[] left, In[] value, In[] right);
}

# region Less Than
public record LessThanOne<T>(int LeftIndex, T Right) : BoolFunction, IFilterFunctionOne<T>
    where T : INumber<T>
{
    public bool[] Ok(T[] left)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] < Right;
        }
        return result;
    }
}

public record LessThanEqualOne<T>(int LeftIndex, T Right) : BoolFunction, IFilterFunctionOne<T>
    where T : INumber<T>
{
    public bool[] Ok(T[] left)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] <= Right;
        }
        return result;
    }
}

public record LessThanTwo<T>(int LeftIndex, int RightIndex) : BoolFunction, IFilterFunctionTwo<T>
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

public record LessThanEqualTwo<T>(int LeftIndex, int RightIndex) : BoolFunction, IFilterFunctionTwo<T>
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

public record EqualOne<T>(int LeftIndex, T Right) : BoolFunction, IFilterFunctionOne<T>
    where T : INumber<T>
{
    public bool[] Ok(T[] left)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] == Right;
        }
        return result;
    }
}

public record EqualTwo<T>(int LeftIndex, int RightIndex) : BoolFunction, IFilterFunctionTwo<T>
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

public record NotEqualOne<T>(int LeftIndex, T Right) : BoolFunction, IFilterFunctionOne<T>
    where T : INumber<T>
{
    public bool[] Ok(T[] left)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] != Right;
        }
        return result;
    }
}

public record NotEqualTwo<T>(int LeftIndex, int RightIndex) : BoolFunction, IFilterFunctionTwo<T>
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

public record Between<T>(int LeftIndex, int ValueIndex, int RightIndex) : BoolFunction, IFilterThreeColsThree<T>
    where T : INumber<T>
{
    public bool[] Ok(T[] left, T[] values, T[] right)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] >= values[i] && left[i] <= right[i];
        }
        return result;
    }
}

#endregion
