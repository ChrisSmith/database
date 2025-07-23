namespace Database.Core.Execution;

public record RowGroup(List<IColumn> Columns)
{
    public int NumRows => Columns[0].Length;

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

    public static RowGroup FromRows(IReadOnlyList<Row> rows)
    {
        var firstRow = rows[0];
        var numCol = firstRow.Values.Count;
        var columns = new List<IColumn>(numCol);
        for (var i = 0; i < numCol; i++)
        {
            var columnType = firstRow.Values[i]!.GetType();
            columns.Add(IColumn.CreateColumn(columnType, $"col{i}", i, rows.Count));
            var values = (Array)columns[i].ValuesArray;

            for (var j = 0; j < rows.Count; j++)
            {
                var row = rows[j];
                values.SetValue(row.Values[i], j);
            }
        }
        return new RowGroup(columns);
    }

    // TODO add a version that takes a list of indexes

    public RowGroup EmptyWithSchema()
    {
        var newColumns = new List<IColumn>(Columns.Count);
        for (var i = 0; i < Columns.Count; i++)
        {
            var orgColumn = Columns[i];
            var columnType = orgColumn.Type;

            var values = Array.CreateInstance(columnType, 1);

            var column = ColumnHelper.CreateColumn(
                columnType,
                orgColumn.Name,
                i,
                values
            );

            newColumns.Add(column);
        }
        return new RowGroup(newColumns);
    }

    public void SingleRowInto(int index, RowGroup into)
    {
        for (var i = 0; i < Columns.Count; i++)
        {
            var orgColumn = Columns[i];
            var rowValue = orgColumn[index];

            var values = (Array)into.Columns[i].ValuesArray;
            values.SetValue(rowValue, 0);
        }
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
