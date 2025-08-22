using Database.Core.Functions;

namespace Database.Core.Expressions;

// DateOnly
public record GreaterThanTwoDateOnly() : BoolFunction, IFilterFunctionTwo<DateOnly>
{
    public bool[] Ok(DateOnly[] left, DateOnly[] right)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] > right[i];
        }
        return result;
    }
}

public record GreaterThanEqualTwoDateOnly() : BoolFunction, IFilterFunctionTwo<DateOnly>
{
    public bool[] Ok(DateOnly[] left, DateOnly[] right)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] >= right[i];
        }
        return result;
    }
}

public record LessThanTwoDateOnly() : BoolFunction, IFilterFunctionTwo<DateOnly>
{
    public bool[] Ok(DateOnly[] left, DateOnly[] right)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] < right[i];
        }
        return result;
    }
}

public record LessThanEqualTwoDateOnly() : BoolFunction, IFilterFunctionTwo<DateOnly>
{
    public bool[] Ok(DateOnly[] left, DateOnly[] right)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] <= right[i];
        }
        return result;
    }
}

public record EqualTwoDateOnly() : BoolFunction, IFilterFunctionTwo<DateOnly>
{
    public bool[] Ok(DateOnly[] left, DateOnly[] right)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] == right[i];
        }
        return result;
    }
}

public record NotEqualTwoDateOnly() : BoolFunction, IFilterFunctionTwo<DateOnly>
{
    public bool[] Ok(DateOnly[] left, DateOnly[] right)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] != right[i];
        }
        return result;
    }
}

// Date Time
public record GreaterThanTwoDateTime() : BoolFunction, IFilterFunctionTwo<DateTime>
{
    public bool[] Ok(DateTime[] left, DateTime[] right)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] > right[i];
        }
        return result;
    }
}

public record GreaterThanEqualTwoDateTime() : BoolFunction, IFilterFunctionTwo<DateTime>
{
    public bool[] Ok(DateTime[] left, DateTime[] right)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] >= right[i];
        }
        return result;
    }
}

public record LessThanTwoDateTime() : BoolFunction, IFilterFunctionTwo<DateTime>
{
    public bool[] Ok(DateTime[] left, DateTime[] right)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] < right[i];
        }
        return result;
    }
}

public record LessThanEqualTwoDateTime() : BoolFunction, IFilterFunctionTwo<DateTime>
{
    public bool[] Ok(DateTime[] left, DateTime[] right)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] <= right[i];
        }
        return result;
    }
}

public record EqualTwoDateTime() : BoolFunction, IFilterFunctionTwo<DateTime>
{
    public bool[] Ok(DateTime[] left, DateTime[] right)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] == right[i];
        }
        return result;
    }
}

public record NotEqualTwoDateTime() : BoolFunction, IFilterFunctionTwo<DateTime>
{
    public bool[] Ok(DateTime[] left, DateTime[] right)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] != right[i];
        }
        return result;
    }
}
