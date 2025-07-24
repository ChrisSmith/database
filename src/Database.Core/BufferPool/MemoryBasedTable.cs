using Database.Core.Execution;

namespace Database.Core.BufferPool;

public class MemoryBasedTable(int NumColumns)
{
    private Dictionary<int, IColumn[]> _rowGroups { get; set; } = new();

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
