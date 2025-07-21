using Database.Core.Catalog;

namespace Database.Core.Execution;

public interface IColumn
{
    string Name { get; }
    int Index { get; }
    Type Type { get; }

    int Length { get; }

    object? this[int index] { get; }

    object ValuesArray { get; }

    public static IColumn CreateColumn(Type dataType, string name, int index, object[] values)
    {
        var columnType = typeof(Column<>).MakeGenericType(dataType);

        return (IColumn)columnType.GetConstructors().Single().Invoke([
            name,
            index,
            values
        ]);
    }

    public static IColumn CreateColumn(Type dataType, string name, int index, int length)
    {
        var values = Array.CreateInstance(dataType, length);
        var columnType = typeof(Column<>).MakeGenericType(dataType);

        return (IColumn)columnType.GetConstructors().Single().Invoke([
            name,
            index,
            values
        ]);
    }
}

public record Column<T>(string Name, int Index, T[] Values) : IColumn
{
    public Type Type => typeof(T);

    public int Length => Values.Length;

    public object? this[int index] => Values[index];
    public object ValuesArray => Values;
}
