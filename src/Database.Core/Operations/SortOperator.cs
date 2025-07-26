using System.Numerics;
using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;

namespace Database.Core.Operations;

public record SortOperator(
    ParquetPool BufferPool,
    MemoryBasedTable MemoryTable,
    IOperation Source,
    IReadOnlyList<OrderingExpression> OrderExpressions,
    List<ColumnSchema> OutputColumns,
    List<ColumnRef> OutputColumnRefs
    ) : IOperation
{
    bool _done = false;

    public RowGroup? Next()
    {
        if (_done)
        {
            return null;
        }

        var allRows = new List<Row>();
        var next = Source.Next();
        while (next != null)
        {
            allRows.AddRange(next.MaterializeRows(BufferPool));
            next = Source.Next();
        }
        _done = true;

        // TODO pull these out of the materialized expressions
        var columns = new List<int>() { 0, 1 };
        var asArray = allRows.ToArray();
        Array.Sort(asArray, new RowComparer(columns));

        return FromRows(asArray);
    }

    private RowGroup FromRows(IReadOnlyList<Row> rows)
    {
        var targetRowGroup = MemoryTable.AddRowGroup();

        for (var i = 0; i < OutputColumns.Count; i++)
        {
            var columnSchema = OutputColumns[i];
            var columnType = columnSchema.ClrType;
            var values = Array.CreateInstance(columnType, rows.Count);
            for (var j = 0; j < rows.Count; j++)
            {
                var row = rows[j];
                values.SetValue(row.Values[i], j);
            }

            var column = ColumnHelper.CreateColumn(
                columnType,
                columnSchema.Name,
                values
            );
            BufferPool.WriteColumn(columnSchema.ColumnRef, column, targetRowGroup.RowGroup);
        }
        return new RowGroup(rows.Count, targetRowGroup, OutputColumnRefs);
    }

    public class RowComparer(List<int> indexes) : IComparer<Row>
    {
        public int Compare(Row x, Row y)
        {
            for (var i = 0; i < indexes.Count; i++)
            {
                var index = indexes[i];
                var xVal = (IComparable)x.Values[index]!;
                var yVal = (IComparable)y.Values[index]!;

                var res = xVal.CompareTo(yVal);
                if (res != 0)
                {
                    return res;
                }
            }
            return 0;
        }
    }
}
