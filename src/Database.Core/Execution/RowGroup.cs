using Database.Core.BufferPool;

namespace Database.Core.Execution;

public record RowGroup(
    int NumRows,
    RowGroupRef RowGroupRef,
    IReadOnlyList<ColumnRef> Columns
    )
{
    public int NumColumns => Columns.Count;

    public List<Row> MaterializeRows(ParquetPool bufferPool)
    {
        var rows = new List<Row>(NumRows);
        for (var i = 0; i < NumRows; i++)
        {
            rows.Add(new Row(new List<object?>(Columns.Count)));
        }

        for (var i = 0; i < Columns.Count; i++)
        {
            var columnRef = Columns[i];
            var column = bufferPool.GetColumn(columnRef with { RowGroup = RowGroupRef.RowGroup });

            for (var j = 0; j < NumRows; j++)
            {
                var row = rows[j];
                row.Values.Add(column[j]);
            }
        }
        return rows;
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
