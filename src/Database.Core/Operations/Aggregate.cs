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

        var states = new List<IAggregateState>(aggregates.Count);
        for (var i = 0; i < aggregates.Count; i++)
        {
            var func = (IAggregateFunction)aggregates[i].BoundFunction!;
            states.Add(func.Initialize());
        }

        var rowGroup = Source.Next();
        while (rowGroup != null)
        {
            for (var i = 0; i < aggregates.Count; i++)
            {
                var expression = aggregates[i];
                var aggregate = (IAggregateFunction)expression.BoundFunction!;
                var aggFunctionExpr = (FunctionExpression)expression;

                _interpreter.ExecuteAggregate(aggFunctionExpr, aggregate, rowGroup, states[i]);
            }

            rowGroup = Source.Next();
        }

        var result = new RowGroup(new List<IColumn>(Expressions.Count));
        for (var i = 0; i < Expressions.Count; i++)
        {
            var expression = Expressions[i];
            var state = states[i];
            var value = ((IAggregateFunction)expression.BoundFunction!).GetValue(state);

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
