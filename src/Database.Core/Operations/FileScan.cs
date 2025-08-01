using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Parquet;
using Parquet.Schema;

namespace Database.Core.Operations;

public record FileScan(
    ParquetPool BufferPool,
    string Path,
    IReadOnlyList<ColumnSchema> OutputColumns,
    IReadOnlyList<ColumnRef> OutputColumnRefs)
    : BaseOperation(OutputColumns, OutputColumnRefs)
{
    private ParquetReader? _reader = null;
    private int _group = -1;
    private bool _done = false;
    private ParquetFileHandle _handle;

    public override RowGroup? Next()
    {
        if (_done)
        {
            return null;
        }

        if (_reader == null)
        {
            _handle = BufferPool.OpenFile(Path);
            _reader = _handle.Reader;
        }

        _group++;
        if (_group >= _reader.RowGroupCount)
        {
            _done = true;
            return null;
        }

        var rg = _reader.OpenRowGroupReader(_group);

        return new RowGroup(
            (int)rg.RowCount,
            new RowGroupRef(_group),
            OutputColumnRefs
        );
    }
}
