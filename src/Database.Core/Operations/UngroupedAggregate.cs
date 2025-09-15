using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;
using Database.Core.Planner;

namespace Database.Core.Operations;

public record UngroupedAggregate(
    ParquetPool BufferPool,
    MemoryBasedTable MemoryTable,
    IOperation Source,
    IReadOnlyList<BaseExpression> Expressions,
    IReadOnlyList<ColumnSchema> OutputColumns,
    IReadOnlyList<ColumnRef> OutputColumnRefs,
    CostEstimate CostEstimate
    ) : BaseOperation(OutputColumns, OutputColumnRefs, CostEstimate)
{
    private bool _done = false;

    private ExpressionInterpreter _interpreter = new ExpressionInterpreter();

    public override void Reset()
    {
        Source.Reset();
        MemoryTable.Truncate();
        _done = false;
    }

    public override RowGroup? Next(CancellationToken token)
    {
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

        var rowGroup = Source.Next(token);
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

                _interpreter.ExecuteAggregate(aggFunctionExpr, aggregate, rowGroup, stateArray, token);
            }

            rowGroup = Source.Next(token);
        }


        var targetRowGroup = MemoryTable.AddRowGroup();

        for (var i = 0; i < Expressions.Count; i++)
        {
            var expression = Expressions[i];
            var state = states[i][0];
            var value = ((IAggregateFunction)expression.BoundFunction!).GetValue(state);

            var columnType = value!.GetType();
            var values = Array.CreateInstance(columnType, 1);
            values.SetValue(value, 0);

            var column = ColumnHelper.CreateColumn(
                columnType,
                expression.Alias,
                values);

            var outputRef = OutputColumnRefs[i];
            BufferPool.WriteColumn(outputRef, column, targetRowGroup.RowGroup);
        }

        _done = true;
        return new RowGroup(1, targetRowGroup, OutputColumnRefs);
    }

    public override Cost EstimateCost()
    {
        var sourceCost = Source.EstimateCost();
        return sourceCost.Add(new Cost(
            OutputRows: CostEstimate.OutputCardinality,
            CpuOperations: sourceCost.OutputRows * Columns.Count,
            DiskOperations: 0
        ));
    }
}
