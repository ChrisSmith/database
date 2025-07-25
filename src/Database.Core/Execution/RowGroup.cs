namespace Database.Core.Execution;

public record RowGroup(
    int NumRows,
    RowGroupRef RowGroupRef,
    List<ColumnRef> Columns
    )
{
    public int NumColumns => Columns.Count;
    // public int NumRows => rows.Count;

    public List<Row> MaterializeRows()
    {
        var numRows = 0;
        var rows = new List<Row>(numRows);
        for (var i = 0; i < numRows; i++)
        {
            // rows.Add(new Row(new List<object?>(Columns.Count)));
        }

        // for (var i = 0; i < Columns.Count; i++)
        // {
        //     var column = Columns[i];
        //
        //     for (var j = 0; j < numRows; j++)
        //     {
        //         var row = rows[j];
        //         row.Values.Add(column[j]);
        //     }
        // }
        return rows;
    }

    public static RowGroup FromRows(IReadOnlyList<Row> rows)
    {
        throw new NotImplementedException();

        var firstRow = rows[0];
        var numCol = firstRow.Values.Count;
        var columns = new List<IColumn>(numCol);
        for (var i = 0; i < numCol; i++)
        {
            var columnType = firstRow.Values[i]!.GetType();
            columns.Add(IColumn.CreateColumn(columnType, $"col{i}", rows.Count));
            var values = (Array)columns[i].ValuesArray;

            for (var j = 0; j < rows.Count; j++)
            {
                var row = rows[j];
                values.SetValue(row.Values[i], j);
            }
        }
        //return new RowGroup(columns);
    }

    // TODO add a version that takes a list of indexes
}

public record MaterializedRowGroup(List<IColumn> Columns)
{
    public int NumRows => Columns[0].Length;

    public List<Row> MaterializeRows()
    {
        var numRows = NumRows;
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
    public static List<Row> AsRowList(this List<MaterializedRowGroup> rowGroups)
    {
        return rowGroups.AsRows().ToList();
    }

    public static IEnumerable<Row> AsRows(this List<MaterializedRowGroup> rowGroups)
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
