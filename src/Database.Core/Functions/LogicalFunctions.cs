namespace Database.Core.Functions;

public record LogicalAnd() : BoolFunction(), IFilterFunctionTwo<bool>
{
    public bool[] Ok(bool[] left, bool[] right)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] && right[i];
        }
        return result;
    }
}

public record LogicalOr() : BoolFunction(), IFilterFunctionTwo<bool>
{
    public bool[] Ok(bool[] left, bool[] right)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] || right[i];
        }
        return result;
    }
}

public record LogicalNot() : BoolFunction(), IFilterFunctionOne<bool>
{
    public bool[] Ok(bool[] values)
    {
        var result = new bool[values.Length];
        for (var i = 0; i < values.Length; i++)
        {
            result[i] = !values[i];
        }
        return result;
    }
}
