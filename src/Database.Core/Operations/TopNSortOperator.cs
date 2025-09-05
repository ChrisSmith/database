using System.Numerics;
using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;

namespace Database.Core.Operations;


public record TopNSortOperator(
    int Limit,
    ParquetPool BufferPool,
    MemoryBasedTable MemoryTable,
    IOperation Source,
    IReadOnlyList<OrderingExpression> OrderExpressions,
    // List<ColumnSchema> SortColumns,
    List<ColumnSchema> OutputColumns,
    List<ColumnRef> OutputColumnRefs
    ) : BaseOperation(OutputColumns, OutputColumnRefs)
{
    bool _done = false;

    private ExpressionInterpreter _interpreter = new ExpressionInterpreter();

    public override void Reset()
    {
        Source.Reset();
        MemoryTable.Truncate();
        _done = false;
    }

    public override RowGroup? Next(CancellationToken token)
    {
        if (_done)
        {
            return null;
        }

        var keyTypes = OrderExpressions.Select(e => e.BoundDataType!.Value.ClrTypeFromDataType()).ToList();
        var sortOrder = OrderExpressions.Select(e => e.Ascending ? SortOrder.Ascending : SortOrder.Descending).ToList();
        var top = new TopHeap<RowRef>(keyTypes, sortOrder, Limit);

        var next = Source.Next(token);

        var currentKeys = new Array[OrderExpressions.Count];
        while (next != null)
        {
            for (var i = 0; i < OrderExpressions.Count; i++)
            {
                var expression = OrderExpressions[i];
                var column = _interpreter.Execute(expression, next, token);
                currentKeys[i] = column.ValuesArray;
            }

            var rowArray = new RowRef[next.NumRows];
            for (var i = 0; i < next.NumRows; i++)
            {
                rowArray[i] = new RowRef(next.RowGroupRef, i);
            }

            top.Insert(currentKeys, rowArray);

            next = Source.Next(token);
        }

        _done = true;

        var asArray = top.ToArray();
        return FromRows(asArray);
    }

    private RowGroup FromRows(IReadOnlyList<RowRef> rows)
    {
        var targetRowGroup = MemoryTable.AddRowGroup();

        var inputColumns = Source.ColumnRefs;

        for (var i = 0; i < OutputColumns.Count; i++)
        {
            var columnSchema = OutputColumns[i];
            var columnType = columnSchema.ClrType;
            var values = Array.CreateInstance(columnType, rows.Count);
            for (var j = 0; j < rows.Count; j++)
            {
                var rowRef = rows[j];

                var columnRef = inputColumns[i];
                var sourceCol = BufferPool.GetColumn(columnRef with { RowGroup = rowRef.RowGroup.RowGroup });

                values.SetValue(sourceCol[rowRef.Row], j);
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
            OutputRows: Limit,
            CpuOperations: (long)sortCost,
            DiskOperations: 0
        ));
    }
}
