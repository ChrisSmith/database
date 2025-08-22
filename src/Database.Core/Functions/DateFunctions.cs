using Database.Core.Catalog;

namespace Database.Core.Functions;

public record CreateDate() : IScalarMathOne<string, DateOnly>
{
    public DataType ReturnType => DataType.Date;

    public DateOnly[] Execute(string[] values)
    {
        var result = new DateOnly[values.Length];
        for (var i = 0; i < values.Length; i++)
        {
            result[i] = DateOnly.Parse(values[i]);
        }
        return result;
    }
}

public record CreateDateTime() : IScalarMathOne<string, DateTime>
{
    public DataType ReturnType => DataType.DateTime;

    public DateTime[] Execute(string[] values)
    {
        var result = new DateTime[values.Length];
        for (var i = 0; i < values.Length; i++)
        {
            result[i] = DateTime.Parse(values[i]);
        }
        return result;
    }
}
