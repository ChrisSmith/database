using Database.Core.Catalog;

namespace Database.Core.Execution;

public record RowGroup(List<IColumn> Columns)
{
    public List<Row> MaterializeRows()
    {
        var numRows = Columns[0].Length;
        var rows = new List<Row>(numRows);
        for (var i = 0; i < numRows; i++)
        {
            rows.Add(new Row(new List<object?>(Columns.Count)));
        }

        for (var i = 0; i < Columns.Count; i++)
        {
            var column = Columns[i];

            for (var j = 0; j < numRows; j++)
            {
                var row = rows[j];
                row.Values.Add(column[j]);
            }
        }
        return rows;
    }
}

public static class RowGroupExtensions
{
    public static List<Row> AsRowList(this List<RowGroup> rowGroups)
    {
        return rowGroups.AsRows().ToList();
    }

    public static IEnumerable<Row> AsRows(this List<RowGroup> rowGroups)
    {
        foreach (var rowGroup in rowGroups)
        {
            foreach (var row in rowGroup.MaterializeRows())
            {
                yield return row;
            }
        }
    }
}
