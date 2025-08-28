using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Operations;

namespace Database.Core.Functions;

public record UnboundCorrelatedSubQueryFunction : IFunction
{
    public DataType ReturnType => DataType.Unknown;
}

public record CorrelatedSubQueryFunction(
    IReadOnlyList<ColumnSchema> SourceInputColumns,
    IReadOnlyList<ColumnSchema> SubQueryCopyInputColumns,
    DataType ReturnType,
    MemoryBasedTable InputTable,
    ParquetPool BufferPool,
    IOperation SubQuery)
    : IFunctionWithRowGroup
{
    private bool _initialized = false;

    private Dictionary<object, object> _cache = new();

    public IColumn Execute(RowGroup rowGroup, CancellationToken token)
    {
        if (!_initialized)
        {
            if (SourceInputColumns.Count != SubQueryCopyInputColumns.Count || SourceInputColumns.Count == 0)
            {
                throw new Exception($"Source and Subquery input columns must be the same length and greater than zero. Got {SourceInputColumns.Count} vs {SubQueryCopyInputColumns.Count}");
            }

            for (var i = 0; i < SourceInputColumns.Count; i++)
            {
                var srcCol = SourceInputColumns[i];
                var dstCol = SubQueryCopyInputColumns[i];
                if (srcCol.DataType != dstCol.DataType)
                {
                    throw new Exception($"Source and Subquery input columns must be the same type. Got {srcCol.DataType} vs {dstCol.DataType}");
                }
            }
            _initialized = true;
        }


        var numRows = rowGroup.NumRows;
        var dataType = ReturnType.ClrTypeFromDataType();
        var outputArray = Array.CreateInstance(dataType, numRows);

        var sourceColumns = new List<IColumn>(SourceInputColumns.Count);
        foreach (var sourceColRef in SourceInputColumns)
        {
            var column = BufferPool.GetColumn(sourceColRef.ColumnRef with { RowGroup = rowGroup.RowGroupRef.RowGroup });
            sourceColumns.Add(column);
        }

        for (var i = 0; i < numRows; i++)
        {
            InputTable.Truncate();
            SubQuery.Reset();
            token.ThrowIfCancellationRequested();

            if (sourceColumns.Count == 1)
            {
                var key = sourceColumns[0][i];
                if (_cache.TryGetValue(key!, out var scalar))
                {
                    outputArray.SetValue(scalar, i);
                    continue;
                }
            }

            // Copy over input var
            for (var j = 0; j < sourceColumns.Count; j++)
            {
                var srcColumn = sourceColumns[j];
                var srcCopy = SubQueryCopyInputColumns[j];
                var rid = InputTable.AddRowGroup().RowGroup;
                var array = Array.CreateInstance(srcCopy.ClrType, 1);
                array.SetValue(srcColumn[i], 0);
                var column = ColumnHelper.CreateColumn(srcCopy.ClrType, srcCopy.Name, array);
                BufferPool.WriteColumn(srcCopy.ColumnRef, column, rid);
            }

            // Execute Subquery
            var next = SubQuery.Next(token);
            if (next != null)
            {
                var sourceColumnRef = next.Columns.Single();
                var column = BufferPool.GetColumn(sourceColumnRef with
                {
                    RowGroup = next.RowGroupRef.RowGroup,
                });
                var scalar = column.ValuesArray.GetValue(0);
                outputArray.SetValue(scalar, i);

                if (sourceColumns.Count == 1)
                {
                    var key = sourceColumns[0][i];
                    _cache[key!] = scalar!;
                }
            }
            else
            {
                // TODO null values?

                if (sourceColumns.Count == 1)
                {
                    var key = sourceColumns[0][i];
                    _cache[key!] = null!;
                }
            }
        }

        return ColumnHelper.CreateColumn(
            dataType,
            "subquery_res",
            outputArray);
    }
}
