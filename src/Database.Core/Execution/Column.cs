namespace Database.Core.Execution;

public interface IColumn
{
    string Name { get; }
    int Index { get; }
    Type Type { get; }

    int Length { get; }

    object? this[int index] { get; }

    object ValuesArray { get; }
}

public record Column<T>(string Name, int Index, T[] Values) : IColumn
{
    public Type Type => typeof(T);

    public int Length => Values.Length;

    public object? this[int index] => Values[index];
    public object ValuesArray => Values;
}
