using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;

namespace Database.Core.Operations;

public record ProjectionOperation(
    ParquetPool BufferPool,
    MemoryBasedTable MemoryTable,
    IOperation Source,
    IReadOnlyList<BaseExpression> Expressions,
    IReadOnlyList<ColumnSchema> OutputColumns,
    IReadOnlyList<ColumnRef> OutputColumnRefs,
    bool Materialize)
    : BaseOperation(OutputColumns, OutputColumnRefs)
{
    private ExpressionInterpreter _interpreter = new();

    public override RowGroup? Next()
    {
        var next = Source.Next();
        if (next == null)
        {
            return null;
        }

        // TODO this doesn't feel quite right
        var targetRg = Materialize
            ? MemoryTable.AddRowGroup()
            : next.RowGroupRef;

        for (var i = 0; i < Expressions.Count; i++)
        {
            var expr = Expressions[i];
            var fun = expr.BoundFunction!;

            // Other functions will need to be materialized
            // Drop them into the buffer pool
            var columnRes = _interpreter.Execute(expr, next);
            var column = ColumnHelper.CreateColumn(
                fun.ReturnType.ClrTypeFromDataType(),
                expr.Alias,
                columnRes.ValuesArray
            );
            var columnRef = OutputColumnRefs[i];
            BufferPool.WriteColumn(columnRef, column, targetRg.RowGroup);
        }

        return new RowGroup(
            next.NumRows,
            targetRg,
            OutputColumnRefs
            );
    }
}
