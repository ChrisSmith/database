using System.Text.RegularExpressions;
using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;

namespace Database.Core.Operations;

public record HashAggregate(
    ParquetPool BufferPool,
    IOperation Source,
    MemoryBasedTable GroupingTable,
    List<BaseExpression> OutputExpressions, // grouping columns + aggregates
    List<ColumnSchema> OutputColumns,
    List<ColumnRef> OutputColumnRefs,
    MemoryBasedTable OutputTable2,
    List<BaseExpression> OutputExpressions2, // projection expressions
    List<ColumnSchema> OutputColumns2,
    List<ColumnRef> OutputColumnRefs2
    ) : IOperation
{
    private bool _done = false;

    private ExpressionInterpreter _interpreter = new ExpressionInterpreter();

    public RowGroup? Next()
    {
        if (_done)
        {
            return null;
        }

        var aggregates = OutputExpressions
            .Where(e => e.BoundFunction is IAggregateFunction)
            .ToList();

        var groupingExpressions = OutputExpressions
            .Where(e => e.BoundFunction is not IAggregateFunction)
            .ToList();

        var rowGroup = Source.Next();

        var hashToAggState = new Dictionary<Row, List<IAggregateState>>();

        while (rowGroup != null)
        {
            // Build up the grouping keys, one per row
            var groupingKeys = new List<Row>(rowGroup.NumRows);
            for (var i = 0; i < rowGroup.NumRows; i++)
            {
                groupingKeys.Add(new Row(new List<object?>(groupingExpressions.Count)));
            }

            for (var g = 0; g < groupingExpressions.Count; g++)
            {
                var expression = groupingExpressions[g];
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
        var groupedRowGroup = FromRows(resRows);

        var outputRowGroup = OutputTable2.AddRowGroup();

        var aggIdx = 0;

        // TODO I think I can remove the projection here entirely now
        for (var i = 0; i < OutputExpressions2.Count; i++)
        {
            var expression = OutputExpressions2[i];
            var columnSchema = OutputColumns2[i];
            var column = IColumn.CreateColumn(columnSchema.ClrType, columnSchema.Name, resRows.Count);
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

            BufferPool.WriteColumn(columnSchema.ColumnRef, column, outputRowGroup.RowGroup);
        }

        _done = true;

        return new RowGroup(
            groupedRowGroup.NumRows,
            outputRowGroup,
            OutputColumnRefs2);
    }

    private RowGroup FromRows(List<KeyValuePair<Row, List<IAggregateState>>> resRows)
    {
        var rows = resRows.Select(kvp => kvp.Key).ToList();
        var targetRowGroup = GroupingTable.AddRowGroup();
        var groupingIdx = 0;
        var aggIdx = 0;

        for (var i = 0; i < OutputExpressions.Count; i++)
        {
            var expression = OutputExpressions[i];
            var columnSchema = OutputColumns[i];
            var columnRef = columnSchema.ColumnRef;
            var columnType = columnSchema.ClrType;
            var values = Array.CreateInstance(columnType, rows.Count);

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
                for (var j = 0; j < rows.Count; j++)
                {
                    var row = rows[j];
                    values.SetValue(row.Values[groupingIdx], j);
                }

                groupingIdx++;
            }

            var column = ColumnHelper.CreateColumn(
                columnType,
                columnSchema.Name,
                values
            );
            BufferPool.WriteColumn(columnRef, column, targetRowGroup.RowGroup);
        }




        return new RowGroup(rows.Count, targetRowGroup, OutputColumnRefs);
    }
}
