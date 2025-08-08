using Database.Core.Catalog;

namespace Database.Core.Functions;

public interface IFunctionTwo<TLeft, TRight, TRes> : IFunction
{
    TRes[] Execute(TLeft[] left, TRight[] right);
}
public record ExtractPart : IFunctionTwo<string, DateTime, int>
{
    public int[] Execute(string[] left, DateTime[] right)
    {
        var result = new int[left.Length];
        for (var i = 0; i < left.Length; i++)
        {
            var part = left[i];
            result[i] = part switch
            {
                "year" => right[i].Year,
                "month" => right[i].Month,
                "day" => right[i].Day,
                "hour" => right[i].Hour,
                "minute" => right[i].Minute,
                "second" => right[i].Second,
                _ => throw new Exception($"Unknown part {part}")
            };
        }
        return result;
    }

    public DataType ReturnType => DataType.Int;
}
