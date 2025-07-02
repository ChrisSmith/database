using Database.Core.Execution;

namespace Database.Core.Operations;

public record Distinct(IOperation Source) : IOperation
{
    private HashSet<Row> _unique = null;

    public RowGroup? Next()
    {
        var rowGroup = Source.Next();
        if (rowGroup == null)
        {
            return null;
        }

        var schema = rowGroup.Schema;
        var columns = schema.Columns;
        var numColumns = columns.Count;

        // What are some better options here?
        // Partition & Sort?
        if (_unique == null)
        {
            _unique = new HashSet<Row>();

            while (rowGroup != null)
            {
                foreach (var row in rowGroup.MaterializeRows())
                {
                    _unique.Add(row);
                }
                rowGroup = Source.Next();
            }
        }

        if (_unique.Count == 0)
        {
            return null;
        }

        var uniqueList = _unique.ToList();

        var result = new RowGroup(schema, new List<IColumn>(numColumns));
        for (var i = 0; i < numColumns; i++)
        {
            var columnType = columns[i].ClrType;
            var values = Array.CreateInstance(columnType, uniqueList.Count);

            for (var j = 0; j < uniqueList.Count; j++)
            {
                var row = uniqueList[j];
                values.SetValue(Convert.ChangeType(row.Values[i], columnType), j);
            }

            var type = typeof(Column<>).MakeGenericType(columnType);
            var column = type.GetConstructors().Single().Invoke([values]);
            result.Columns.Add((IColumn)column);
        }

        // How do we determine the chunk size now?
        return result;
    }
}
