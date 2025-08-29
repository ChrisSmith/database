using System.Collections;
using System.Diagnostics;
using Database.Core.Catalog;
using Database.Core.Execution;

namespace Database.Core.BufferPool;

[DebuggerDisplay("{_storage}")]
public class MemoryBasedTable(MemoryStorage storage)
{
    private readonly MemoryStorage _storage = storage;

    private List<int> _rowGroupIndexes { get; set; } = new();
    private List<IColumn[]> _rowGroups { get; set; } = new();
    private List<ColumnSchema> _schema = new();

    public int NumColumns => _schema.Count;
    public IReadOnlyList<ColumnSchema> Schema => _schema;

    private static volatile int _nextRowGroup = 0;

    public RowGroupRef AddRowGroup()
    {
        var id = Interlocked.Decrement(ref _nextRowGroup);
        return new RowGroupRef(id);
    }

    public ColumnSchema AddColumnToSchema(string name, DataType type, string tableName, string tableAlias)
    {
        if (name == "")
        {
            throw new Exception("Column name cannot be empty");
        }

        var columnId = NumColumns;
        var columnRef = new ColumnRef(storage, -1, columnId);
        var newColumn = new ColumnSchema(
            columnRef,
            (ColumnId)columnId,
            name,
            type,
            type.ClrTypeFromDataType(),
            SourceTableName: tableName,
            SourceTableAlias: tableAlias
            );
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

        var rowGroupIndex = GetRowGroupIndex(columnRef.RowGroup);
        if (rowGroupIndex == -1)
        {
            throw new Exception($"Row group {columnRef} not found from {_rowGroups.Count} row groups.");
        }
        var rowGroup = _rowGroups[rowGroupIndex];

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
            throw new Exception($"Attempting to write a column {column.Name} of type {column.Type} to a column {columnSchema.Name} of type {columnSchema.ClrType}");
        }

        var rowGroupIndex = GetRowGroupIndex(columnRef.RowGroup);
        IColumn[] rowGroup;
        if (rowGroupIndex == -1)
        {
            rowGroup = new IColumn[NumColumns];
            _rowGroupIndexes.Add(columnRef.RowGroup);
            _rowGroups.Add(rowGroup);
        }
        else
        {
            rowGroup = _rowGroups[rowGroupIndex];
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

    private int GetRowGroupIndex(int rowGroup)
    {
        for (var i = 0; i < _rowGroupIndexes.Count; i++)
        {
            if (_rowGroupIndexes[i] == rowGroup)
            {
                return i;
            }
        }
        return -1;
    }

    public IReadOnlyList<int> GetRowGroups()
    {
        return _rowGroupIndexes;
    }

    public void Truncate()
    {
        _rowGroupIndexes.Clear();
        _rowGroups.Clear();
    }
}
