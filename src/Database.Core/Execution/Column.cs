namespace Database.Core.Execution;

public interface IColumn
{
    int Length { get; }
}

public record Column<T>(T[] Values) : IColumn
{
    public int Length => Values.Length;
}
