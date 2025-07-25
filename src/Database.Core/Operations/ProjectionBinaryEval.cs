using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;

namespace Database.Core.Operations;

public class ProjectionBinaryEval(ParquetPool BufferPool, IOperation Source, List<IExpression> expressions) : IOperation
{
    private ExpressionInterpreter _interpreter = new ExpressionInterpreter();

    public RowGroup? Next()
    {
        var next = Source.Next();
        if (next is null)
        {
            return null;
        }

        var newColumns = new List<ColumnRef>(next.Columns);
        var rowGroup = next.RowGroupRef.RowGroup;

        for (var i = 0; i < expressions.Count; i++)
        {
            var expr = expressions[i];
            var fun = expr.BoundFunction!;

            var columnRes = _interpreter.Execute(expr, next);
            var column = ColumnHelper.CreateColumn(
                fun.ReturnType.ClrTypeFromDataType(),
                expr.Alias,
                i,
                columnRes.ValuesArray
            );

            var columnRef = expr.BoundOutputColumn;
            BufferPool.WriteColumn(columnRef, column, rowGroup);
            newColumns.Add(columnRef);
        }

        return new RowGroup(
            next.NumRows,
            next.RowGroupRef,
            newColumns
        );
    }
}
