using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;

namespace Database.Core.Functions;

public record SelectSubQueryFunction(ColumnRef ColumnRef, DataType ReturnType, ParquetPool BufferPool)
    : IFunctionWithColumnLength
{
    public IColumn Execute(int length)
    {
        var column = BufferPool.GetColumn(ColumnRef with { RowGroup = 0 });
        if (column.Length != 1)
        {
            throw new Exception($"Scalar Subquery must return a single value, got {column.Length}");
        }
        var scalar = column[0];

        var type = ReturnType.ClrTypeFromDataType();
        var outputArray = Array.CreateInstance(type, length);
        for (var i = 0; i < length; i++)
        {
            outputArray.SetValue(scalar, i);
        }

        return ColumnHelper.CreateColumn(
            type,
            column.Name,
            outputArray);
    }
}
