using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;

namespace Database.Core.Operations;

public record Filter(IOperation Source, IExpression Expression) : IOperation
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

            int count = 0;
            for (var i = 0; i < keep.Length; i++)
            {
                count += keep[i] ? 1 : 0;
            }

            if (count == 0)
            {
                // filtered all rows out, try to obtain the next batch
                continue;
            }
            if (next.NumRows == count)
            {
                // No filtering necessary
                return next;
            }

            var numColumns = next.Columns.Count;
            var columns = next.Columns;
            var result = new RowGroup(new List<IColumn>(numColumns));
            for (var i = 0; i < numColumns; i++)
            {
                var columnType = columns[i].Type;
                var values = Array.CreateInstance(columnType, count);
                var column = ColumnHelper.CreateColumn(
                    columnType,
                    columns[i].Name,
                    i,
                    values
                );

                column.SetValues(next.Columns[i].ValuesArray, keep);
                result.Columns.Add(column);
            }

            return result;
        }
    }
}
