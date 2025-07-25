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
        throw new NotImplementedException();
        /**
        if (_done)
        {
            return null;
        }

        var aggregates = Expressions
            .Where(e => e.BoundFunction is IAggregateFunction)
            .ToList();

        var states = new List<IAggregateState[]>(aggregates.Count);
        for (var i = 0; i < aggregates.Count; i++)
        {
            var func = (IAggregateFunction)aggregates[i].BoundFunction!;
            var arr = func.InitializeArray(1);
            arr[0] = func.Initialize();
            states.Add(arr);
        }

        var rowGroup = Source.Next();
        while (rowGroup != null)
        {
            for (var i = 0; i < aggregates.Count; i++)
            {
                var expression = aggregates[i];
                var aggregate = (IAggregateFunction)expression.BoundFunction!;
                var aggFunctionExpr = (FunctionExpression)expression;

                // We have the same state for each row
                // TODO Consider an optimized version for aggregates with no groupings
                var stateArray = aggregate.InitializeArray(rowGroup.NumRows);
                for (var j = 0; j < rowGroup.NumRows; j++)
                {
                    stateArray[j] = states[i][0];
                }

                _interpreter.ExecuteAggregate(aggFunctionExpr, aggregate, rowGroup, stateArray);
            }

            rowGroup = Source.Next();
        }

        var result = new RowGroup(new List<IColumn>(Expressions.Count));
        for (var i = 0; i < Expressions.Count; i++)
        {
            var expression = Expressions[i];
            var state = states[i][0];
            var value = ((IAggregateFunction)expression.BoundFunction!).GetValue(state);

            var columnType = value!.GetType();
            var values = Array.CreateInstance(columnType, 1);
            values.SetValue(Convert.ChangeType(value, columnType), 0);

            var column = ColumnHelper.CreateColumn(columnType, expression.Alias, i, values);
            result.Columns.Add(column);
        }

        _done = true;
        return result;
        **/
    }
}
