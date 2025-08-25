using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;

namespace Database.Core.Operations;

public record DistinctOperation(
    ParquetPool BufferPool,
    MemoryBasedTable MemoryTable,
    IOperation Source,
    List<ColumnSchema> OutputColumns,
    List<ColumnRef> OutputColumnRefs
    ) : BaseOperation(OutputColumns, OutputColumnRefs)
{
    private HashSet<Row> _unique = null;

    public override void Reset()
    {
        Source.Reset();
        MemoryTable.Truncate();
        _unique = null;
    }

    public override RowGroup? Next()
    {
        var rowGroup = Source.Next();
        if (rowGroup == null)
        {
            return null;
        }

        var columns = rowGroup.Columns;
        var numColumns = columns.Count;

        // What are some better options here?
        // Partition & Sort?
        if (_unique == null)
        {
            _unique = new HashSet<Row>();

            while (rowGroup != null)
            {
                foreach (var row in rowGroup.MaterializeRows(BufferPool))
                {
                    _unique.Add(row);
                }
                rowGroup = Source.Next();
            }
        }

        if (_unique.Count == 0)
        {
            return null;
        }

        var uniqueList = _unique.ToList();
        var targetRowGroup = MemoryTable.AddRowGroup();

        for (var i = 0; i < numColumns; i++)
        {
            var outputColumn = OutputColumns[i];
            var columnType = outputColumn.ClrType;
            var values = Array.CreateInstance(columnType, uniqueList.Count);

            for (var j = 0; j < uniqueList.Count; j++)
            {
                var row = uniqueList[j];
                values.SetValue(row.Values[i], j);
            }

            var column = ColumnHelper.CreateColumn(
                columnType,
                outputColumn.Name,
                values
            );

            BufferPool.WriteColumn(outputColumn.ColumnRef, column, targetRowGroup.RowGroup);
        }

        return new RowGroup(uniqueList.Count, targetRowGroup, OutputColumnRefs);
    }

    public override Cost EstimateCost()
    {
        var sourceCost = Source.EstimateCost();
        // TODO distinct estimate
        return sourceCost.Add(new Cost(
            OutputRows: sourceCost.OutputRows,
            CpuOperations: sourceCost.OutputRows * Columns.Count,
            DiskOperations: 0
        ));
    }
}
