using Database.Core.Catalog;
using Database.Core.Execution;

namespace Database.Core.Operations;

public record Projection(List<string> Columns, List<int> ColumnIndexes, IOperation Source) : IOperation
{
    public RowGroup? Next()
    {
        var rowGroup = Source.Next();
        if (rowGroup == null)
        {
            return null;
        }

        var newColumns = new List<IColumn>(Columns.Count);

        for (var i = 0; i < Columns.Count; i++)
        {
            var columnIdx = ColumnIndexes[i];
            var columnName = Columns[i];
            var oldColumn = rowGroup.Columns[columnIdx];

            var type = typeof(Column<>).MakeGenericType(oldColumn.Type);
            var column = type.GetConstructors().Single().Invoke([
                columnName,
                i,
                oldColumn.ValuesArray
            ]);
            newColumns.Add((IColumn)column);
        }

        var newRowGroup = new RowGroup(newColumns);
        return newRowGroup;
    }
}
