using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;

namespace Database.Core.Operations;

public record Projection(TableSchema Schema, IOperation Source, List<IExpression> Expressions) : IOperation
{
    private ExpressionInterpreter _interpreter = new ExpressionInterpreter();

    public RowGroup? Next()
    {
        var rowGroup = Source.Next();
        if (rowGroup == null)
        {
            return null;
        }

        var newColumns = new List<IColumn>(Expressions.Count);

        for (var i = 0; i < Expressions.Count; i++)
        {
            var expr = Expressions[i];
            var fun = expr.BoundFunction!;

            var columnRes = _interpreter.Execute(expr, rowGroup);
            var column = ColumnHelper.CreateColumn(
                fun.ReturnType.ClrTypeFromDataType(),
                expr.Alias,
                i,
                columnRes.ValuesArray
            );
            newColumns.Add(column);
        }

        var newRowGroup = new RowGroup(newColumns);
        return newRowGroup;
    }
}
