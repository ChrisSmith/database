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

            if (fun is ScalarMathOneLeft<int> sli)
            {
                var leftIdx = sli.LeftIndex;
                var left = next.Columns[leftIdx];
                outputArray = sli.Execute((int[])left.ValuesArray);
            }
            else if (fun is ScalarMathOneRight<int> sri)
            {
                var rightIdx = sri.RightIndex;
                var right = next.Columns[rightIdx];
                outputArray = sri.Execute((int[])right.ValuesArray);
            }
            else if (fun is ScalarMathTwo<int> sti)
            {
                var leftIdx = sti.LeftIndex;
                var left = next.Columns[leftIdx];
                var rightIdx = sti.RightIndex;
                var right = next.Columns[rightIdx];
                outputArray = sti.Execute((int[])left.ValuesArray, (int[])right.ValuesArray);
            }
            else if (fun is ScalarMathOneLeft<long> sll)
            {
                var leftIdx = sll.LeftIndex;
                var left = next.Columns[leftIdx];
                outputArray = sll.Execute((long[])left.ValuesArray);
            }
            else if (fun is ScalarMathOneRight<long> srl)
            {
                var rightIdx = srl.RightIndex;
                var right = next.Columns[rightIdx];
                outputArray = srl.Execute((long[])right.ValuesArray);
            }
            else if (fun is ScalarMathTwo<long> stl)
            {
                var leftIdx = stl.LeftIndex;
                var left = next.Columns[leftIdx];
                var rightIdx = stl.RightIndex;
                var right = next.Columns[rightIdx];
                outputArray = stl.Execute((long[])left.ValuesArray, (long[])right.ValuesArray);
            }
            else if (fun is ScalarMathOneLeft<float> slf)
            {
                var leftIdx = slf.LeftIndex;
                var left = next.Columns[leftIdx];
                outputArray = slf.Execute((float[])left.ValuesArray);
            }
            else if (fun is ScalarMathOneRight<float> srf)
            {
                var rightIdx = srf.RightIndex;
                var right = next.Columns[rightIdx];
                outputArray = srf.Execute((float[])right.ValuesArray);
            }
            else if (fun is ScalarMathTwo<float> stf)
            {
                var leftIdx = stf.LeftIndex;
                var left = next.Columns[leftIdx];
                var rightIdx = stf.RightIndex;
                var right = next.Columns[rightIdx];
                outputArray = stf.Execute((float[])left.ValuesArray, (float[])right.ValuesArray);
            }
            else if (fun is ScalarMathOneLeft<double> sld)
            {
                var leftIdx = sld.LeftIndex;
                var left = next.Columns[leftIdx];
                outputArray = sld.Execute((double[])left.ValuesArray);
            }
            else if (fun is ScalarMathOneRight<double> srd)
            {
                var rightIdx = srd.RightIndex;
                var right = next.Columns[rightIdx];
                outputArray = srd.Execute((double[])right.ValuesArray);
            }
            else if (fun is ScalarMathTwo<double> std)
            {
                var leftIdx = std.LeftIndex;
                var left = next.Columns[leftIdx];
                var rightIdx = std.RightIndex;
                var right = next.Columns[rightIdx];
                outputArray = std.Execute((double[])left.ValuesArray, (double[])right.ValuesArray);
            }
            else
            {
                throw new NotImplementedException($"Function {fun.GetType().Name} not implemented");
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
