using System.Diagnostics;
using Database.Core.Catalog;
using Database.Core.Execution;

namespace Database.Core.BufferPool;

[DebuggerDisplay("{storage}")]
public class MemoryBasedTable(MemoryStorage storage)
{
    private Dictionary<int, IColumn[]> _rowGroups { get; set; } = new();
    private List<ColumnSchema> _schema = new();

    public int NumColumns => _schema.Count;
    public IReadOnlyList<ColumnSchema> Schema => _schema;

    private static volatile int _nextRowGroup = 0;

    public RowGroupRef AddRowGroup()
    {
        var id = Interlocked.Decrement(ref _nextRowGroup);
        return new RowGroupRef(id);
    }

    public ColumnSchema AddColumnToSchema(string name, DataType type)
    {
        if (name == "")
        {
            // TODO this currently breaks because I'm not generating unique column names
            // throw new Exception("Column name cannot be empty");
        }

        var columnId = NumColumns;
        var columnRef = new ColumnRef(storage, -1, columnId);
        var newColumn = new ColumnSchema(
            columnRef,
            (ColumnId)columnId,
            name,
            type,
            type.ClrTypeFromDataType());
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
        ValidateColumnReference(columnRef);

        if (!_rowGroups.TryGetValue(columnRef.RowGroup, out var rowGroup))
        {
            throw new Exception($"Row group {columnRef} not found");
        }

        var column = rowGroup[columnRef.Column];
        if (column == null)
        {
            throw new Exception($"Column {columnRef} not found");
        }
        return column;
    }

    public void PutColumn(ColumnRef columnRef, IColumn column)
    {
        ValidateColumnReference(columnRef);

        var columnSchema = _schema[columnRef.Column];
        if (columnSchema.ClrType != column.Type)
        {
            throw new Exception($"Attempting to write a column of type {column.Type} to a column of type {columnSchema.ClrType}");
        }

        if (!_rowGroups.TryGetValue(columnRef.RowGroup, out var rowGroup))
        {
            rowGroup = new IColumn[NumColumns];
            _rowGroups[columnRef.RowGroup] = rowGroup;
        }

        if (rowGroup[columnRef.Column] != null)
        {
            throw new Exception("Cannot overwrite an existing column in row group.");
        }
        rowGroup[columnRef.Column] = column;
    }

    private void ValidateColumnReference(ColumnRef columnRef)
    {
        if (!columnRef.Storage.Equals(storage))
        {
            throw new Exception($"Column ref {columnRef} does not belong to table {storage.TableId}");
        }

        if (_schema.Count <= columnRef.Column)
        {
            throw new Exception($"Attempting to write to a column {columnRef.Column} that does not exist in the schema.");
        }
    }
}
