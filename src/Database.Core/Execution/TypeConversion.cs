using Parquet.Data;
using Parquet.Schema;

namespace Database.Core.Execution;

public static class TypeConversion
{
    public static (Type, Array) ThrowIfNullable(DataColumn column, DataField field)
    {
        if (column.Field.IsNullable && field.ClrType != typeof(string))
        {
            throw new NotImplementedException("Nullable types aren't supported yet");
        }

        return (field.ClrType, column.Data);

        // var targetType = typeof(double);
        // var finalCopy = new double[column.Data.Length];
        // var source = (decimal[])column.Data;
        // for (var j = 0; j < column.Data.Length; j++)
        // {
        //     finalCopy[j] = (double)source[j];
        // }
        // return (targetType, finalCopy);
    }
}
