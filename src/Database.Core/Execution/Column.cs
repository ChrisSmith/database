using System.Reflection;
using Database.Core.Catalog;

namespace Database.Core.Execution;

public interface IColumn
{
    string Name { get; }
    int Index { get; }
    Type Type { get; }

    int Length { get; }

    object? this[int index] { get; }

    Array ValuesArray { get; }

    public static IColumn CreateColumn(Type dataType, string name, int index, int length)
    {
        var values = Array.CreateInstance(dataType, length);

        return ColumnHelper.CreateColumn(
            dataType,
            name,
            index,
            values
        );
    }

    public void SetValues(Array source, bool[] mask);
}

public record Column<T>(string Name, int Index, T[] Values) : IColumn
{
    public Type Type => typeof(T);

    public int Length => Values.Length;

    public object? this[int index] => Values[index];
    public Array ValuesArray => Values;

    public void SetValues(Array source, bool[] mask)
    {
        var sourceArray = (T[])source;
        var idx = 0;
        for (var i = 0; i < source.Length; i++)
        {
            if (mask[i])
            {
                Values[idx] = sourceArray[i];
                idx++;
            }
        }
    }
}

public static class ColumnHelper
{
    // TODO make thread safe
    private static readonly Dictionary<Type, ConstructorInfo> _typeCache = new();

    public static IColumn CreateColumn(Type targetType, string name, int index, Array values)
    {
        if (!_typeCache.TryGetValue(targetType, out var ctor))
        {
            var cachedType = typeof(Column<>).MakeGenericType(targetType);
            ctor = cachedType.GetConstructors().Single();
            _typeCache[targetType] = ctor;
        }

        return (IColumn)ctor.Invoke([
            name,
            index,
            values
        ]);
    }
}
