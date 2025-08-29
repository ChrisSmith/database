using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Parquet;
using Parquet.Schema;

namespace Database.Core.Operations;

public record ScanMemoryTable(
    MemoryBasedTable Table,
    Catalog.Catalog Catalog,
    IReadOnlyList<ColumnSchema> OutputColumns,
    IReadOnlyList<ColumnRef> OutputColumnRefs)
    : BaseOperation(OutputColumns, OutputColumnRefs)
{
    private IReadOnlyList<int>? _rowGroups = null;
    private int _group = -1;
    private bool _done = false;

    public override void Reset()
    {
        _done = false;
    }

    public override RowGroup? Next(CancellationToken token)
    {
        if (_done)
        {
            return null;
        }

        if (_rowGroups == null)
        {
            _rowGroups = Table.GetRowGroups();
        }

        _group++;
        if (_group >= _rowGroups.Count)
        {
            _done = true;
            return null;
        }

        var rg = _rowGroups[_group];
        var column = Table.GetColumn(Table.Schema[0].ColumnRef with { RowGroup = rg });

        return new RowGroup(
            column.Length,
            new RowGroupRef(rg),
            OutputColumnRefs
        );
    }

    public override Cost EstimateCost()
    {
        // var table = Catalog.GetTable()

        return new Cost(
            OutputRows: 0,
            CpuOperations: 0,
            DiskOperations: 0,
            TotalRowsProcessed: 0,
            TotalCpuOperations: 0,
            TotalDiskOperations: 0
        );
    }
}
