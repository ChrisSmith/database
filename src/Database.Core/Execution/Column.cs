namespace Database.Core.Execution;

public interface IColumn
{
    Type Type { get; }

    int Length { get; }

    object? this[int index] { get; }
}

public record Column<T>(T[] Values) : IColumn
{
    public Type Type => typeof(T);

    public int Length => Values.Length;

    public object? this[int index] => Values[index];
}
