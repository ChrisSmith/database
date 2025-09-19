using Database.Core.Types;
using Parquet.Data;
using Parquet.Schema;

namespace Database.Core.Execution;

public static class TypeConversion
{
    public static Type ConvertIfNecessary(Type type)
    {
        if (type == typeof(decimal))
        {
            return typeof(Decimal15);
        }

        return type;
    }


    public static (Type, Array) ConvertIfNecessary(DataColumn column, DataField field)
    {
        if (column.Field.IsNullable && field.ClrType != typeof(string))
        {
            throw new NotImplementedException("Nullable types aren't supported yet");
        }

        if (field.ClrType == typeof(decimal))
        {
            var source = (decimal[])column.Data;
            var finalCopy = new Decimal15[source.Length];
            for (var j = 0; j < source.Length; j++)
            {
                finalCopy[j] = new Decimal15(source[j]);
            }
            return (typeof(Decimal15), finalCopy);
        }

        return (field.ClrType, column.Data);
    }
}
