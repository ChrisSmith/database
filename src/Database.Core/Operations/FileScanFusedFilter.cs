using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;
using Parquet;
using Parquet.Schema;

namespace Database.Core.Operations;

public record FileScanFusedFilter(
    ParquetPool BufferPool,
    string Path,
    Catalog.Catalog Catalog,
    BaseExpression Expression,
    IReadOnlyList<ColumnSchema> OutputColumns,
    IReadOnlyList<ColumnRef> OutputColumnRefs
    )
    : BaseOperation(OutputColumns, OutputColumnRefs)
{
    private ParquetReader? _reader = null;
    private TableSchema _table;
    private int _groupIdx = -1;
    private bool _done = false;
    private ParquetFileHandle _handle;
    private ExpressionInterpreter _interpreter = new();
    private List<int> RowGroupsKeep;

    public override RowGroup? Next()
    {
        if (_done)
        {
            return null;
        }

        if (_reader == null)
        {
            _handle = BufferPool.OpenFile(Path);
            _table = Catalog.Tables.Single(t => t.Location == Path);
            _reader = _handle.Reader;

            // Create columns of data based purely on the statistic
            // Then execute the expression against them

            var statsRgRef = _table.StatsRowGroup;
            var res = (Column<bool>)_interpreter.Execute(Expression, statsRgRef);
            var keep = res.Values;

            RowGroupsKeep = new List<int>(res.Values.Length);
            for (var i = 0; i < keep.Length; i++)
            {
                if (keep[i])
                {
                    RowGroupsKeep.Add(i);
                }
            }
        }

        _groupIdx++;
        if (_groupIdx >= RowGroupsKeep.Count)
        {
            _done = true;
            return null;
        }

        var group = RowGroupsKeep[_groupIdx];
        var rg = _reader.OpenRowGroupReader(group);

        return new RowGroup(
            (int)rg.RowCount,
            new RowGroupRef(group),
            OutputColumnRefs
        );
    }
}
