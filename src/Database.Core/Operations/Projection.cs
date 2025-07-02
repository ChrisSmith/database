using Database.Core.Catalog;
using Database.Core.Execution;

namespace Database.Core.Operations;

public record Projection(List<string> Columns, IOperation Source) : IOperation
{
    public RowGroup? Next()
    {
        var rowGroup = Source.Next();
        if (rowGroup == null || Columns.Count == 0)
        {
            return null;
        }

        var columnNames = rowGroup.Schema.Columns.Select(c => c.Name).ToList();
        if (Columns.Count == rowGroup.Columns.Count && Columns.SequenceEqual(columnNames))
        {
            return rowGroup;
        }

        var newColumns = new List<IColumn>(Columns.Count);
        var columnSchema = new List<ColumnSchema>(Columns.Count);
        var newSchema = new Schema(columnSchema);

        foreach (var column in Columns)
        {
            var idx = columnNames.IndexOf(column);
            if (idx == -1)
            {
                throw new InvalidOperationException($"Column {column} not found in source. Available columns: {string.Join(", ", columnNames)}");
            }
            newColumns.Add(rowGroup.Columns[idx]);
            columnSchema.Add(rowGroup.Schema.Columns[idx]);
        }

        var newRowGroup = new RowGroup(newSchema, newColumns);
        return newRowGroup;
    }
}
