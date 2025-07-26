using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;

namespace Database.Core.Operations;

public record Projection(
    ParquetPool BufferPool,
    MemoryBasedTable MemoryTable,
    IOperation Source,
    IReadOnlyList<BaseExpression> Expressions) : IOperation
{
    private ExpressionInterpreter _interpreter = new();

    public RowGroup? Next()
    {
        var rowGroup = Source.Next();
        if (rowGroup == null)
        {
            return null;
        }

        var newColumns = new List<ColumnRef>(Expressions.Count);
        var group = rowGroup.RowGroupRef.RowGroup;

        for (var i = 0; i < Expressions.Count; i++)
        {
            var expr = Expressions[i];
            var fun = expr.BoundFunction!;

            // Other functions will need to be materialized
            // Drop them into the buffer pool
            var columnRes = _interpreter.Execute(expr, rowGroup);
            var column = ColumnHelper.CreateColumn(
                fun.ReturnType.ClrTypeFromDataType(),
                expr.Alias,
                columnRes.ValuesArray
            );
            var columnRef = expr.BoundOutputColumn;
            BufferPool.WriteColumn(columnRef, column, group);
            newColumns.Add(columnRef);
        }

        return new RowGroup(
            rowGroup.NumRows,
            rowGroup.RowGroupRef,
            newColumns
            );
    }
}
