using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;
using Database.Core.Planner;

namespace Database.Core.Operations;

public record NestedLoopJoinOperator(
    ParquetPool BufferPool,
    IOperation LeftSource,
    IOperation RightSource,
    MemoryBasedTable Table,
    BaseExpression? Expression,
    List<ColumnSchema> OutputColumns,
    List<ColumnRef> OutputColumnRefs,
    CostEstimate CostEstimate
) : BaseOperation(OutputColumns, OutputColumnRefs, CostEstimate)
{
    private bool _done = false;
    private ExpressionInterpreter _interpreter = new ExpressionInterpreter();
    private List<RowGroup>? _rightRows = null;
    private RowGroup? _leftNext = null;
    private int _rightIndex = 0;

    public override void Reset()
    {
        _done = false;
        _rightIndex = 0;
        _leftNext = null;
        _rightRows = null;
        LeftSource.Reset();
        RightSource.Reset();
        Table.Truncate();
    }

    public override RowGroup? Next(CancellationToken token)
    {
        if (_done)
        {
            return null;
        }

        // assume the right is the smaller table
        if (_rightRows == null)
        {
            _rightRows = new List<RowGroup>();
            var rightNextInner = RightSource.Next(token);
            while (rightNextInner != null)
            {
                _rightRows.Add(rightNextInner);
                rightNextInner = RightSource.Next(token);
            }
        }

        if (_rightIndex >= _rightRows.Count || _leftNext == null)
        {
            _rightIndex = 0;
            _leftNext = LeftSource.Next(token);
            if (_leftNext == null)
            {
                _done = true;
                return null;
            }
        }

        var rightNext = _rightRows[_rightIndex];
        _rightIndex++;

        var targetRg = Table.AddRowGroup();
        var count = 0;

        var leftColumns = LeftSource.Columns;
        var rightColumns = RightSource.Columns;

        if (Expression == null)
        {
            count = checked(_leftNext.NumRows * rightNext.NumRows);

            var columnValues = new IColumn[leftColumns.Count + rightColumns.Count];
            for (var c = 0; c < leftColumns.Count; c++)
            {
                var columnRef = OutputColumnRefs[c];
                var columnType = leftColumns[c].ClrType;
                var values = Array.CreateInstance(columnType, count);
                var column = ColumnHelper.CreateColumn(
                    columnType,
                    leftColumns[c].Name,
                    values
                );

                columnValues[c] = column;
                BufferPool.WriteColumn(columnRef, column, targetRg.RowGroup);
            }
            // Do they need to be separate lists? maybe combine them
            for (var c = 0; c < rightColumns.Count; c++)
            {
                var columnRef = OutputColumnRefs[c + leftColumns.Count];
                var columnType = rightColumns[c].ClrType;
                var values = Array.CreateInstance(columnType, count);
                var column = ColumnHelper.CreateColumn(
                    columnType,
                    rightColumns[c].Name,
                    values
                );

                columnValues[c + leftColumns.Count] = column;
                BufferPool.WriteColumn(columnRef, column, targetRg.RowGroup);
            }

            for (var c = 0; c < leftColumns.Count; c++)
            {
                var sourceColRef = leftColumns[c].ColumnRef;
                var sourceCol = BufferPool.GetColumn(sourceColRef with { RowGroup = _leftNext.RowGroupRef.RowGroup });

                var outputArray = columnValues[c].ValuesArray;

                for (var i = 0; i < _leftNext.NumRows; i++)
                {
                    for (var j = 0; j < rightNext.NumRows; j++)
                    {
                        outputArray.SetValue(sourceCol[i], i * rightNext.NumRows + j);
                    }
                }
            }

            for (var c = 0; c < rightColumns.Count; c++)
            {
                var sourceColRef = rightColumns[c].ColumnRef;
                var sourceCol = BufferPool.GetColumn(sourceColRef with { RowGroup = rightNext.RowGroupRef.RowGroup });

                var outputArray = columnValues[c + leftColumns.Count].ValuesArray;

                for (var i = 0; i < _leftNext.NumRows; i++)
                {
                    for (var j = 0; j < rightNext.NumRows; j++)
                    {
                        outputArray.SetValue(sourceCol[j], i * rightNext.NumRows + j);
                    }
                }
            }
        }
        else
        {
            // We need a way to either
            // 1. execute on two row groups at once
            // 2. join the rowgroups
            // _interpreter.Execute()
            throw new NotImplementedException("nested loop with expression not implemented yet");
        }


        return new RowGroup(count, targetRg, OutputColumnRefs);
    }

    public override Cost EstimateCost()
    {
        var leftSource = LeftSource.EstimateCost();
        var rightSource = RightSource.EstimateCost();

        return leftSource.Add(new Cost(
            OutputRows: CostEstimate.OutputCardinality,
            CpuOperations: CostEstimate.OutputCardinality,
            DiskOperations: rightSource.DiskOperations,
            TotalCpuOperations: rightSource.TotalCpuOperations,
            TotalDiskOperations: rightSource.TotalDiskOperations,
            TotalRowsProcessed: rightSource.TotalRowsProcessed
        ));
    }
}
