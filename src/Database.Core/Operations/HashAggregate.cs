using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;

namespace Database.Core.Operations;

public record HashAggregate(IOperation Source, List<IExpression> Expressions, List<IExpression> GroupingExpressions) : IOperation
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

        var hashToAggState = new Dictionary<Row, List<IAggregateState>>();

        while (rowGroup != null)
        {
            // Build up the grouping keys, one per row
            var groupingKeys = new List<Row>(rowGroup.NumRows);
            for (var i = 0; i < rowGroup.NumRows; i++)
            {
                groupingKeys.Add(new Row(new List<object?>(GroupingExpressions.Count)));
            }

            for (var g = 0; g < GroupingExpressions.Count; g++)
            {
                var expression = GroupingExpressions[g];
                var column = _interpreter.Execute(expression, rowGroup);
                for (var i = 0; i < column.Length; i++)
                {
                    groupingKeys[i].Values.Add(column[i]);
                }
            }

            // This is by aggregate column
            var stateArray = new List<IAggregateState[]>(aggregates.Count);
            for (var i = 0; i < aggregates.Count; i++)
            {
                var fun = (IAggregateFunction)aggregates[i].BoundFunction!;
                stateArray.Add(fun.InitializeArray(rowGroup.NumRows));
            }

            // For each row, get the list of aggregate states and put them into
            // a row oriented list. This means the same state can be in the list multiple times
            for (var i = 0; i < groupingKeys.Count; i++)
            {
                var groupingKey = groupingKeys[i];
                if (!hashToAggState.TryGetValue(groupingKey, out var states))
                {
                    states = new List<IAggregateState>(aggregates.Count);
                    for (var a = 0; a < aggregates.Count; a++)
                    {
                        var aggregate = (IAggregateFunction)aggregates[a].BoundFunction!;
                        states.Add(aggregate.Initialize());
                    }
                    hashToAggState[groupingKey] = states;
                }

                for (var a = 0; a < aggregates.Count; a++)
                {
                    stateArray[a][i] = states[a];
                }
            }

            // Update all aggregate states. Since these are pointers to the global states,
            // they're updating the global stats too
            for (var a = 0; a < aggregates.Count; a++)
            {
                var expression = aggregates[a];
                var aggregate = (IAggregateFunction)expression.BoundFunction!;
                var aggFunctionExpr = (FunctionExpression)expression;
                var state = stateArray[a];

                _interpreter.ExecuteAggregate(aggFunctionExpr, aggregate, rowGroup, state);
            }

            rowGroup = Source.Next();
        }

        var resRows = hashToAggState.ToList();
        var rows = resRows.Select(kvp => kvp.Key).ToList();
        var groupedRowGroup = RowGroup.FromRows(rows);

        var aggIdx = 0;
        // create empty rowgroup we'll fill in below
        var result = new RowGroup(new List<IColumn>(Expressions.Count));
        for (var i = 0; i < Expressions.Count; i++)
        {
            var expression = Expressions[i];
            var fun = expression.BoundFunction!;
            var dataType = fun.ReturnType.ClrTypeFromDataType();
            var valuesArray = Array.CreateInstance(dataType, resRows.Count);
            var column = ColumnHelper.CreateColumn(dataType, expression.Alias, i, valuesArray);
            result.Columns.Add(column);
        }

        // Fill in the rowgroup with the aggregate results
        for (var i = 0; i < Expressions.Count; i++)
        {
            var expression = Expressions[i];
            var column = result.Columns[i];
            var values = column.ValuesArray;

            if (expression.BoundFunction is IAggregateFunction aggFn)
            {
                for (var j = 0; j < resRows.Count; j++)
                {
                    var (key, states) = resRows[j];
                    var state = states[aggIdx];
                    var value = aggFn.GetValue(state);
                    values.SetValue(value, j);
                }
                aggIdx++;
            }
            else
            {
                var columnRes = _interpreter.Execute(expression, groupedRowGroup);
                Array.Copy(columnRes.ValuesArray, values, columnRes.Length);
            }
        }

        _done = true;
        return result;
    }
}
