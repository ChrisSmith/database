using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Functions;

namespace Database.Core.Operations;

public class ProjectionBinaryEval(TableSchema Schema, IOperation Source, List<IFunction> functions) : IOperation
{
    public RowGroup? Next()
    {
        var next = Source.Next();
        if (next is null)
        {
            return null;
        }
        var newColumns = new List<IColumn>(Schema.Columns.Count);
        for (var i = 0; i < Schema.Columns.Count - functions.Count; i++)
        {
            var column = next.Columns[i];
            newColumns.Add(column);
        }

        for (var i = 0; i < functions.Count; i++)
        {
            var fun = functions[i];
            var type = typeof(Column<>).MakeGenericType(fun.ReturnType.ClrTypeFromDataType());

            object outputArray = null;

            if (fun is ScalarMathOneCommutative<int> scalar)
            {
                var leftIdx = scalar.Index;
                var left = next.Columns[leftIdx];
                outputArray = scalar.Execute((int[])left.ValuesArray);
            }

            var column = type.GetConstructors().Single().Invoke([
                $"fun+{i}",
                i,
                outputArray
            ]);
            newColumns.Add((IColumn)column);
        }

        var newRowGroup = new RowGroup(newColumns);
        return newRowGroup;
    }
}
