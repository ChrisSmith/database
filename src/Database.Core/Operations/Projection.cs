using Database.Core.Execution;

namespace Database.Core.Operations;

public record Projection(List<string> Columns, IOperation Source): IOperation
{
    public RowGroup? Next()
    {
        var rowGroup = Source.Next();
        if (rowGroup == null || Columns.Count == 0)
        {
            return null;
        }

        if (Columns.Count == rowGroup.Columns.Count && Columns.SequenceEqual(rowGroup.ColumnNames))
        {
            return rowGroup;
        }
        
        var newColumns = new List<IColumn>(Columns.Count);
        
        foreach(var column in Columns)
        {
            var idx = rowGroup.ColumnNames.IndexOf(column);
            if (idx == -1)
            {
                throw new InvalidOperationException($"Column {column} not found in source");
            }
            newColumns.Add(rowGroup.Columns[idx]);
        }
        
        var newRowGroup = new RowGroup(Columns, newColumns);
        return newRowGroup;
    }
}
