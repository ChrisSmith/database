using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;

namespace Database.Core.Functions;

public record SelectSubQueryFunction(ColumnRef ColumnRef, DataType ReturnType, MemoryBasedTable Table, ParquetPool BufferPool)
    : IFunctionWithColumnLength
{
    public IColumn Execute(int length)
    {
        var rowGroups = Table.GetRowGroups();

        var type = ReturnType.ClrTypeFromDataType();
        var outputArray = Array.CreateInstance(type, length);
        var columnSchema = Table.GetColumnSchema(ColumnRef);

        object? scalar = null;

        // hack for not-exists subquery eval, really need null support
        if (rowGroups.Count == 0 && ReturnType == DataType.Bool)
        {
            scalar = false;
        }
        else if (rowGroups.Count == 1)
        {
            var column = BufferPool.GetColumn(ColumnRef with { RowGroup = rowGroups[0] });
            if (column.Length != 1)
            {
                throw new Exception($"Scalar Subquery must return a single value, got {column.Length}");
            }
            scalar = column[0];
        }
        else
        {
            throw new Exception($"Scalar Subquery must return a single value, got {rowGroups.Count} rowGroups");
        }

        for (var i = 0; i < length; i++)
        {
            outputArray.SetValue(scalar, i);
        }

        return ColumnHelper.CreateColumn(
            type,
            columnSchema.Name,
            outputArray);
    }
}
