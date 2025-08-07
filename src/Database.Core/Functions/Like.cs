using System.Text.RegularExpressions;

namespace Database.Core.Functions;

public record DynamicLike() : BoolFunction(), IFilterFunctionTwo<string>
{
    public bool[] Ok(string[] left, string[] right)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            var regex = StringToRegex(right[i]);
            result[i] = regex.IsMatch(left[i]);
        }
        return result;
    }

    public static Regex StringToRegex(string pattern)
    {
        if (!pattern.Contains("%"))
        {
            return new Regex(pattern, RegexOptions.IgnoreCase);
        }

        var updatedPattern = pattern.Replace("%", ".*");
        return new Regex(updatedPattern, RegexOptions.IgnoreCase);
    }
}

// TODO hook these up during expression simplification or something

public record StaticLike(Regex Regex) : BoolFunction(), IFilterFunctionOne<string>
{
    public bool[] Ok(string[] values)
    {
        var result = new bool[values.Length];
        for (var i = 0; i < values.Length; i++)
        {
            result[i] = Regex.IsMatch(values[i]);
        }
        return result;
    }
}

public record StartsWith(string Prefix) : BoolFunction(), IFilterFunctionOne<string>
{
    public bool[] Ok(string[] values)
    {
        var result = new bool[values.Length];
        for (var i = 0; i < values.Length; i++)
        {
            result[i] = values[i].StartsWith(Prefix, StringComparison.OrdinalIgnoreCase);
        }
        return result;
    }
}

public record EndsWith(string Suffix) : BoolFunction(), IFilterFunctionOne<string>
{
    public bool[] Ok(string[] values)
    {
        var result = new bool[values.Length];
        for (var i = 0; i < values.Length; i++)
        {
            result[i] = values[i].EndsWith(Suffix, StringComparison.OrdinalIgnoreCase);
        }
        return result;
    }
}

public record Contains(string Needle) : BoolFunction(), IFilterFunctionOne<string>
{
    public bool[] Ok(string[] values)
    {
        var result = new bool[values.Length];
        for (var i = 0; i < values.Length; i++)
        {
            result[i] = values[i].Contains(Needle, StringComparison.OrdinalIgnoreCase);
        }
        return result;
    }
}
