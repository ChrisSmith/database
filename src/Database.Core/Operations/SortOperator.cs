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
    List<ColumnSchema> SortColumns,
    List<ColumnSchema> OutputColumns,
    List<ColumnRef> OutputColumnRefs
    ) : BaseOperation(OutputColumns, OutputColumnRefs)
{
    bool _done = false;

    private ExpressionInterpreter _interpreter = new ExpressionInterpreter();

    public override RowGroup? Next()
    {
        if (_done)
        {
            return null;
        }

        var allRows = new List<Row>();
        var next = Source.Next();

        while (next != null)
        {
            var updatedColumnRefs = new List<ColumnRef>(next.Columns);
            for (var i = 0; i < OrderExpressions.Count; i++)
            {
                var expression = OrderExpressions[i];
                var column = _interpreter.Execute(expression, next);

                var columnRef = SortColumns[i].ColumnRef;
                BufferPool.WriteColumn(columnRef, column, next.RowGroupRef.RowGroup);
                updatedColumnRefs.Add(columnRef);
            }

            // Need to re-write this operator to use a b+ tree
            var updatedRg = next with { Columns = updatedColumnRefs };
            allRows.AddRange(updatedRg.MaterializeRows(BufferPool));
            next = Source.Next();
        }
        _done = true;

        var asArray = allRows.ToArray();
        Array.Sort(asArray, new RowComparer(Source.Columns.Count, OrderExpressions));

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

    public override Cost EstimateCost()
    {
        var sourceCost = Source.EstimateCost();
        var sortCost = sourceCost.OutputRows * BigInteger.Log2(sourceCost.OutputRows) * OrderExpressions.Count;

        return sourceCost.Add(new Cost(
            OutputRows: sourceCost.OutputRows,
            CpuOperations: (long)sortCost,
            DiskOperations: 0
        ));
    }

    public class RowComparer(int offset, IReadOnlyList<OrderingExpression> expressions) : IComparer<Row>
    {
        public int Compare(Row x, Row y)
        {
            for (var i = 0; i < expressions.Count; i++)
            {
                var index = offset + i;
                var xVal = (IComparable)x.Values[index]!;
                var yVal = (IComparable)y.Values[index]!;

                if (expressions[i].Ascending)
                {
                    var res = xVal.CompareTo(yVal);
                    if (res != 0)
                    {
                        return res;
                    }
                }
                else
                {
                    var res = yVal.CompareTo(xVal);
                    if (res != 0)
                    {
                        return res;
                    }
                }
            }
            return 0;
        }
    }
}
