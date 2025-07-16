using Database.Core.Catalog;
using Database.Core.Execution;

namespace Database.Core.Functions;

public record SelectFunction(int Index, DataType ReturnType) : IFunction
{
    public IColumn SelectColumn(RowGroup rowGroup)
    {
        return rowGroup.Columns[Index];
    }
}

public record LiteralFunction(int Index, object Value, DataType ReturnType) : IFunction
{
    public IColumn MaterializeColumn(int length)
    {
        var type = ReturnType.ClrTypeFromDataType();
        var outputArray = Array.CreateInstance(type, length);
        for (var i = 0; i < length; i++)
        {
            outputArray.SetValue(Value, i);
        }

        var columnType = typeof(Column<>).MakeGenericType(type);

        var column = (IColumn)columnType.GetConstructors().Single().Invoke([
            "foo",
            Index,
            outputArray
        ]);
        return column;
    }
}
