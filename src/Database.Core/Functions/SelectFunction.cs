using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;

namespace Database.Core.Functions;

public record SelectFunction(ColumnRef ColumnRef, DataType ReturnType, ParquetPool BufferPool) : IFunctionWithRowGroup
{
    public IColumn Execute(RowGroup rowGroup)
    {
        var column = BufferPool.GetColumn(ColumnRef with
        {
            RowGroup = rowGroup.RowGroupRef.RowGroup,
        });
        return column;
    }
}

public record LiteralFunction(object Value, DataType ReturnType) : IFunctionWithColumnLength
{
    public IColumn Execute(int length)
    {
        var type = ReturnType.ClrTypeFromDataType();
        var outputArray = Array.CreateInstance(type, length);
        for (var i = 0; i < length; i++)
        {
            outputArray.SetValue(Value, i);
        }

        var column = ColumnHelper.CreateColumn(
            type,
            "foo",
            outputArray);

        return column;
    }
}
