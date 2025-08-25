using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;

namespace Database.Core.Operations;

public record LimitOperator(
    ParquetPool BufferPool,
    MemoryBasedTable MemoryTable,
    IOperation Source,
    int LimitCount,
    IReadOnlyList<ColumnSchema> OutputColumns,
    IReadOnlyList<ColumnRef> OutputColumnRefs
) : BaseOperation(OutputColumns, OutputColumnRefs)
{
    private bool _done = false;
    private int _count = 0;

    public override void Reset()
    {
        _done = false;
        _count = 0;
        Source.Reset();
        MemoryTable.Truncate();
    }

    public override RowGroup? Next()
    {
        if (_done)
        {
            return null;
        }

        var next = Source.Next();
        if (next == null)
        {
            _done = true;
            return null;
        }

        var columns = next.Columns;
        var sourceRowGroup = next.RowGroupRef.RowGroup;
        var targetRowGroup = MemoryTable.AddRowGroup();

        var take = LimitCount - _count;
        take = Math.Min(take, next.NumRows);
        _count += take;
        if (_count >= LimitCount)
        {
            _done = true;
        }

        var keepMask = new bool[next.NumRows];
        for (var i = 0; i < take; i++)
        {
            keepMask[i] = true;
        }

        for (var i = 0; i < next.Columns.Count; i++)
        {
            var sourceColumn = BufferPool.GetColumn(columns[i] with { RowGroup = sourceRowGroup });
            var columnType = sourceColumn.Type;

            var values = Array.CreateInstance(columnType, take);
            var column = ColumnHelper.CreateColumn(
                columnType,
                sourceColumn.Name,
                values);
            column.SetValues(sourceColumn.ValuesArray, keepMask);

            var outputRef = OutputColumnRefs[i];
            BufferPool.WriteColumn(outputRef, column, targetRowGroup.RowGroup);
        }

        return new RowGroup(
            take,
            targetRowGroup,
            OutputColumnRefs
        );
    }

    public override Cost EstimateCost()
    {
        var sourceCost = Source.EstimateCost();
        return sourceCost.Add(new Cost(
            OutputRows: LimitCount,
            CpuOperations: LimitCount * Columns.Count,
            DiskOperations: 0
        ));
    }
}
