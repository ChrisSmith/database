using Database.Core.BufferPool;
using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;

namespace Database.Core.Operations;

public record Filter(
    ParquetPool BufferPool,
    MemoryBasedTable MemoryTable,
    IOperation Source,
    BaseExpression Expression,
    List<ColumnRef> OutputColumns
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

        while (true)
        {
            var next = Source.Next();
            if (next == null)
            {
                _done = true;
                return null;
            }

            var res = (Column<bool>)_interpreter.Execute(Expression, next);
            var keep = res.Values;

            var count = 0;
            for (var i = 0; i < keep.Length; i++)
            {
                if (keep[i])
                {
                    count++;
                }
            }

            if (count == 0)
            {
                // filtered all rows out, try to obtain the next batch
                continue;
            }

            // TODO think about how we want to deal with columns/rowgroups from separate tables
            // if (next.NumRows == count)
            // {
            //     // No filtering necessary
            //     return next;
            // }

            var columns = next.Columns;
            // TODO allocate a new one?
            var sourceRowGroup = next.RowGroupRef.RowGroup;
            var targetRowGroup = MemoryTable.AddRowGroup();

            if (next.Columns.Count != OutputColumns.Count)
            {
                var got = string.Join(", ", next.Columns);
                var expected = string.Join(", ", OutputColumns);

                var gotSet = new HashSet<ColumnRef>(next.Columns);
                var expectedSet = new HashSet<ColumnRef>(OutputColumns);
                var additional = string.Join(", ", gotSet.Except(expectedSet));
                var missing = string.Join(", ", expectedSet.Except(gotSet));

                throw new Exception($"Filter output columns do not match input columns. " +
                                    $"Got {next.Columns.Count} Expected {OutputColumns.Count}\n" +
                                    $"Additional: {additional}\n" +
                                    $"Missing: {missing}\n" +
                                    $"Got: {got}\n" +
                                    $"Expected: {expected}");
            }

            for (var i = 0; i < next.Columns.Count; i++)
            {
                var sourceColumn = BufferPool.GetColumn(columns[i] with { RowGroup = sourceRowGroup });
                var columnType = sourceColumn.Type;

                var values = Array.CreateInstance(columnType, count);
                var column = ColumnHelper.CreateColumn(
                    columnType,
                    sourceColumn.Name,
                    values);
                column.SetValues(sourceColumn.ValuesArray, keep);

                var outputRef = OutputColumns[i];
                BufferPool.WriteColumn(outputRef, column, targetRowGroup.RowGroup);
            }


            // Are there scenarios where using a mask would be better than doing the copy?
            // This would require the rest of the pipeline be filter aware however
            // which increases the code complexity
            return new RowGroup(
                count,
                targetRowGroup,
                OutputColumns
                );
        }
    }
}
