namespace Database.Core.Functions;

public interface IFilterFunction { }

public interface IFilterFunctionOne<In> : IFilterFunction
{
    public int LeftIndex { get; }

    public bool[] Ok(In[] left);
}

public interface IFilterFunctionTwo<In> : IFilterFunction
{
    public int LeftIndex { get; }

    public int RightIndex { get; }

    public bool[] Ok(In[] left, In[] right);
}

public record IntLessThanOne(int LeftIndex, int Right) : IFilterFunctionOne<int>
{
    public bool[] Ok(int[] left)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] < Right;
        }
        return result;
    }
}


public record IntLessThanTwo(int LeftIndex, int RightIndex) : IFilterFunctionTwo<int>
{
    public bool[] Ok(int[] left, int[] right)
    {
        var result = new bool[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            result[i] = left[i] < right[i];
        }
        return result;
    }
}
