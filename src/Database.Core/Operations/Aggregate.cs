using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;

namespace Database.Core.Operations;

public record Aggregate(IOperation Source, List<AggregateValue> Expressions) : IOperation
{
    private bool _done = false;

    public RowGroup? Next()
    {
        if (_done)
        {
            return null;
        }

        var rowGroup = Source.Next();
        while (rowGroup != null)
        {
            foreach (var expression in Expressions)
            {
                var column = rowGroup.Columns[expression.ColumnIndex];

                switch (expression)
                {
                    case AggregateValue<double?, int> agg when column is Column<double?> c:
                        agg.Next(c.Values);
                        break;
                    case AggregateValue<double, int> agg when column is Column<double> c:
                        agg.Next(c.Values);
                        break;
                    case AggregateValue<int?, int> agg when column is Column<int?> c:
                        agg.Next(c.Values);
                        break;
                    case AggregateValue<int, int> agg when column is Column<int> c:
                        agg.Next(c.Values);
                        break;
                    case AggregateValue<int, double> agg when column is Column<int> c:
                        agg.Next(c.Values);
                        break;
                    case AggregateValue<string, int> agg when column is Column<string> c:
                        agg.Next(c.Values);
                        break;
                    default:
                        throw new NotImplementedException($"Unsupported aggregate type {expression.GetType().FullName}");
                }
            }
            rowGroup = Source.Next();
        }

        var result = new RowGroup(new List<IColumn>(Expressions.Count));
        for (var i = 0; i < Expressions.Count; i++)
        {
            var expression = Expressions[i];
            var value = expression.GetValue();

            var columnType = value!.GetType();
            var values = Array.CreateInstance(columnType, 1);
            values.SetValue(Convert.ChangeType(value, columnType), 0);

            var type = typeof(Column<>).MakeGenericType(columnType);
            var column = type.GetConstructors().Single().Invoke([
                $"{i}.{columnType}",
                i,
                values
            ]);
            result.Columns.Add((IColumn)column);
        }

        _done = true;
        return result;
    }
}
