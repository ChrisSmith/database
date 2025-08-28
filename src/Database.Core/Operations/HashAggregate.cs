using System.Text.RegularExpressions;
using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;
using Database.Core.Planner;

namespace Database.Core.Operations;

public record HashAggregate(
    ParquetPool BufferPool,
    IOperation Source,
    MemoryBasedTable GroupingTable,
    List<BaseExpression> OutputExpressions, // grouping columns + aggregates
    List<ColumnSchema> OutputColumns,
    List<ColumnRef> OutputColumnRefs
    ) : BaseOperation(OutputColumns, OutputColumnRefs)
{
    private bool _done = false;

    private ExpressionInterpreter _interpreter = new ExpressionInterpreter();

    public override void Reset()
    {
        Source.Reset();
        GroupingTable.Truncate();
        _done = false;
    }

    public override RowGroup? Next(CancellationToken token)
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

        var rowGroup = Source.Next(token);

        var keyTypes = groupingExpressions.Select(g => g.BoundDataType!.Value.ClrTypeFromDataType()).ToArray();
        var hashToAggState = new HashTable<List<IAggregateState>>(keyTypes);

        while (rowGroup != null)
        {
            // Build up the grouping keys, one per row
            var groupingKeys = GroupByKeys(rowGroup, groupingExpressions, token);

            // This is by aggregate column
            var stateArray = InitializeAggregateStates(aggregates, rowGroup, groupingKeys, hashToAggState);

            // Update all aggregate states. Since these are pointers to the global states,
            // they're updating the global stats too
            ComputeAggregates(aggregates, stateArray, rowGroup, token);

            rowGroup = Source.Next(token);
        }

        var resRows = hashToAggState.KeyValuePairs();
        var groupedRowGroup = FromRows(resRows);

        _done = true;

        return groupedRowGroup;
    }

    private void ComputeAggregates(List<BaseExpression> aggregates, List<IAggregateState[]> stateArray, RowGroup rowGroup, CancellationToken token)
    {
        for (var a = 0; a < aggregates.Count; a++)
        {
            var expression = aggregates[a];
            var aggregate = (IAggregateFunction)expression.BoundFunction!;
            var aggFunctionExpr = (FunctionExpression)expression;
            var state = stateArray[a];

            _interpreter.ExecuteAggregate(aggFunctionExpr, aggregate, rowGroup, state, token);
        }
    }

    private static List<IAggregateState[]> InitializeAggregateStates(
        IReadOnlyList<BaseExpression> aggregates,
        RowGroup rowGroup,
        IReadOnlyList<IColumn> groupingKeys,
        HashTable<List<IAggregateState>> hashToAggState)
    {
        var count = aggregates.Count;
        var stateArray = new List<IAggregateState[]>(count);
        for (var i = 0; i < count; i++)
        {
            var fun = (IAggregateFunction)aggregates[i].BoundFunction!;
            stateArray.Add(fun.InitializeArray(rowGroup.NumRows));
        }

        var numRows = groupingKeys[0].Length;
        if (numRows != rowGroup.NumRows)
        {
            throw new Exception("Number of rows in grouping keys does not match number of rows in row group");
        }

        var output = hashToAggState.GetOrAdd(groupingKeys, () =>
        {
            var states = new List<IAggregateState>(count);
            for (var a = 0; a < count; a++)
            {
                var aggregate = (IAggregateFunction)aggregates[a].BoundFunction!;
                states.Add(aggregate.Initialize());
            }

            return states;
        });

        for (var i = 0; i < numRows; i++)
        {
            for (var a = 0; a < count; a++)
            {
                stateArray[a][i] = output[i][a];
            }
        }

        return stateArray;
    }

    private List<IColumn> GroupByKeys(RowGroup rowGroup, IReadOnlyList<BaseExpression> groupingExpressions, CancellationToken token)
    {
        var numColumns = groupingExpressions.Count;
        var result = new List<IColumn>(numColumns);

        for (var i = 0; i < numColumns; i++)
        {
            var expression = groupingExpressions[i];
            var column = _interpreter.Execute(expression, rowGroup, token);
            result.Add(column);
        }
        return result;
    }

    private RowGroup FromRows(List<KeyValuePair<List<object?>, List<IAggregateState>>> resRows)
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
                    values.SetValue(row[groupingIdx], j);
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

    public override Cost EstimateCost()
    {
        var sourceCost = Source.EstimateCost();
        var expressionCost = CostEstimation.EstimateExpressionCost(OutputExpressions) * sourceCost.OutputRows;
        // TODO cardinality estimates of the grouping keys
        var numGroups = 10;

        return sourceCost.Add(new Cost(
            OutputRows: numGroups,
            CpuOperations: expressionCost * 2,
            DiskOperations: 0
        ));
    }
}
