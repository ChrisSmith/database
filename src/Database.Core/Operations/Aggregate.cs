using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;

namespace Database.Core.Operations;

public record Aggregate(IOperation Source, List<IExpression> Expressions) : IOperation
{
    private bool _done = false;

    private ExpressionInterpreter _interpreter = new ExpressionInterpreter();

    public RowGroup? Next()
    {
        if (_done)
        {
            return null;
        }

        var aggregates = Expressions
            .Where(e => e.BoundFunction is IAggregateFunction)
            .ToList();

        var rowGroup = Source.Next();
        while (rowGroup != null)
        {
            foreach (var expression in aggregates)
            {
                var aggregate = (IAggregateFunction)expression.BoundFunction!;

                _interpreter.ExecuteAggregate(aggregate, rowGroup);
            }
            rowGroup = Source.Next();
        }

        var result = new RowGroup(new List<IColumn>(Expressions.Count));
        for (var i = 0; i < Expressions.Count; i++)
        {
            var expression = Expressions[i];
            var value = ((IAggregateFunction)expression.BoundFunction!).GetValue();

            var columnType = value!.GetType();
            var values = Array.CreateInstance(columnType, 1);
            values.SetValue(Convert.ChangeType(value, columnType), 0);

            var type = typeof(Column<>).MakeGenericType(columnType);
            var column = type.GetConstructors().Single().Invoke([
                expression.Alias,
                i,
                values
            ]);
            result.Columns.Add((IColumn)column);
        }

        _done = true;
        return result;
    }
}
