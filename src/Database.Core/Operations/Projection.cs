using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;

namespace Database.Core.Operations;

public record Projection(ParquetPool BufferPool, IOperation Source, List<IExpression> Expressions) : IOperation
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

            // Select function references existing data, return a pointer to it
            if (fun is SelectFunction sel)
            {
                newColumns.Add(sel.ColumnRef);
                continue;
            }

            // Other functions will need to be materialized
            // Drop them into the buffer pool
            var columnRes = _interpreter.Execute(expr, rowGroup);
            var column = ColumnHelper.CreateColumn(
                fun.ReturnType.ClrTypeFromDataType(),
                expr.Alias,
                i,
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
