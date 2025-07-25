using Database.Core.Catalog;
using Database.Core.Execution;

namespace Database.Core.BufferPool;

public class MemoryBasedTable(MemoryStorage storage)
{
    private Dictionary<int, IColumn[]> _rowGroups { get; set; } = new();
    private List<ColumnSchema> _schema = new();

    public int NumColumns => _schema.Count;
    public IReadOnlyList<ColumnSchema> Schema => _schema;

    private int _nextRowGroup = 0;

    public RowGroupRef AddRowGroup()
    {
        return new RowGroupRef(--_nextRowGroup);
    }

    public ColumnSchema AddColumnToSchema(string name, DataType type)
    {
        var columnId = NumColumns;
        var columnRef = new ColumnRef(storage, -1, columnId);
        var newColumn = new ColumnSchema(
            columnRef,
            (ColumnId)columnId,
            name,
            type,
            type.ClrTypeFromDataType(),
            -1); // TODO remove all these extra indexes
        _schema.Add(newColumn);
        return newColumn;
    }

    public ColumnSchema GetColumnSchema(ColumnRef columnRef)
    {
        if (!columnRef.Storage.Equals(storage))
        {
            throw new Exception($"Column ref {columnRef} does not belong to table {storage.TableId}");
        }
        return _schema[columnRef.Column];
    }

    public IColumn GetColumn(ColumnRef columnRef)
    {
        if (!_rowGroups.TryGetValue(columnRef.RowGroup, out var rowGroup))
        {
            throw new Exception($"Row group {columnRef} not found");
        }
        return rowGroup[columnRef.Column];
    }

    public void PutColumn(ColumnRef columnRef, IColumn column)
    {
        if (!_rowGroups.TryGetValue(columnRef.RowGroup, out var rowGroup))
        {
            rowGroup = new IColumn[NumColumns];
            _rowGroups[columnRef.RowGroup] = rowGroup;
        }
        rowGroup[columnRef.Column] = column;
    }
}
