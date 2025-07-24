using Database.Core.BufferPool;
using Database.Core.Execution;
using Parquet;
using Parquet.Schema;

namespace Database.Core.Operations;

public record FileScan(ParquetPool BufferPool, string Path) : IOperation
{
    private ParquetReader? _reader = null;
    private int _group = -1;
    private DataField[] _dataFields;
    private bool _done = false;

    public RowGroup? Next()
    {
        if (_done)
        {
            return null;
        }

        if (_reader == null)
        {
            var handle = BufferPool.OpenFile(Path);
            _reader = handle.Reader;
            _dataFields = handle.DataFields;
        }

        _group++;
        if (_group >= _reader.RowGroupCount)
        {
            _done = true;
            return null;
        }

        var rg = _reader.OpenRowGroupReader(_group);

        var columnValues = new List<IColumn>(_dataFields.Length);

        for (var i = 0; i < _dataFields.Length; i++)
        {
            var field = _dataFields[i];
            var column = rg.ReadColumnAsync(field).GetAwaiter().GetResult();
            var (targetType, finalCopy) = TypeConversion.RemoveNullablesHack(column, field);

            var obj = ColumnHelper.CreateColumn(targetType, field.Name, i, finalCopy);

            columnValues.Add(obj);
        }

        return new RowGroup(columnValues);
    }
}
