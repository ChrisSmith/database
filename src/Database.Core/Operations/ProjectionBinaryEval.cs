using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;

namespace Database.Core.Operations;

public class ProjectionBinaryEval(TableSchema Schema, IOperation Source, List<IExpression> expressions) : IOperation
{
    private ExpressionInterpreter _interpreter = new ExpressionInterpreter();

    public RowGroup? Next()
    {
        var next = Source.Next();
        if (next is null)
        {
            return null;
        }
        var newColumns = new List<IColumn>(Schema.Columns.Count);
        var nonExpressionColumns = Schema.Columns.Count - expressions.Count;
        for (var i = 0; i < nonExpressionColumns; i++)
        {
            var column = next.Columns[i];
            newColumns.Add(column);
        }

        for (var i = 0; i < expressions.Count; i++)
        {
            var expr = expressions[i];
            var fun = expr.BoundFunction!;
            var type = typeof(Column<>).MakeGenericType(fun.ReturnType.ClrTypeFromDataType());

            var columnRes = _interpreter.Execute(expr, next);

            var columnInSchema = Schema.Columns[nonExpressionColumns + i];
            var column = type.GetConstructors().Single().Invoke([
                columnInSchema.Name,
                i,
                columnRes.ValuesArray
            ]);
            newColumns.Add((IColumn)column);
        }

        var newRowGroup = new RowGroup(newColumns);
        return newRowGroup;
    }
}
