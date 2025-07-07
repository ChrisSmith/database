using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;

namespace Database.Core.Operations;

public record Filter(IOperation Source, IFilterFunction Expression) : IOperation
{
    private bool _done = false;

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

            bool[] keep;

            if (Expression is IFilterFunctionOne<int> one)
            {
                var left = next.Columns[one.LeftIndex];
                keep = one.Ok((int[])left.ValuesArray);
            }
            else if (Expression is IFilterFunctionTwo<int> twos)
            {
                var left = next.Columns[twos.LeftIndex];
                var right = next.Columns[twos.RightIndex];

                keep = twos.Ok((int[])left.ValuesArray, (int[])right.ValuesArray);
            }
            else
            {
                throw new NotImplementedException(
                    $"Expression {Expression.GetType().FullName} is not supported in a filter yet");
            }

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

                int keepIdx = 0;
                for (var row = 0; row < next.NumRows; row++)
                {
                    if (keep[row])
                    {
                        values.SetValue(Convert.ChangeType(next.Columns[i][row], columnType), keepIdx);
                        keepIdx++;
                    }
                }

                var type = typeof(Column<>).MakeGenericType(columnType);
                var column = type.GetConstructors().Single().Invoke([
                    columns[i].Name,
                    i,
                    values
                ]);
                result.Columns.Add((IColumn)column);
            }

            return result;
        }
    }
}
