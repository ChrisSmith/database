using Parquet.Data;
using Parquet.Schema;

namespace Database.Core.Execution;

public static class TypeConversion
{
    // Theres an issue with my source data where it thinks columns are null where they shouldn't be
    // This casts back to the non-nullable array type
    public static (Type, Array) RemoveNullablesHack(DataColumn column, DataField field)
    {
        var targetType = field.ClrType;
        if (targetType == typeof(decimal))
        {
            targetType = typeof(double);
        }

        var finalCopy = column.Data;
        if (column.Field.IsNullable && targetType != typeof(string))
        {
            if (column.Data is decimal?[] dec)
            {
                var copy = new double[column.Data.Length];
                for (var j = 0; j < column.Data.Length && j < dec.Length; j++)
                {
                    copy[j] = (double)dec[j]!;
                }
                finalCopy = copy;
            }
            else if (column.Data is long?[] decl)
            {
                var copy = new long[column.Data.Length];
                for (var j = 0; j < column.Data.Length && j < decl.Length; j++)
                {
                    copy[j] = (long)decl[j]!;
                }
                finalCopy = copy;
            }
            else if (column.Data is int?[] deci)
            {
                var copy = new int[column.Data.Length];
                for (var j = 0; j < column.Data.Length && j < deci.Length; j++)
                {
                    copy[j] = (int)deci[j]!;
                }
                finalCopy = copy;
            }
            else if (column.Data is DateTime?[] decdt)
            {
                var copy = new DateTime[column.Data.Length];
                for (var j = 0; j < column.Data.Length && j < decdt.Length; j++)
                {
                    copy[j] = (DateTime)decdt[j]!;
                }
                finalCopy = copy;
            }
            else
            {
                throw new NotImplementedException();
                finalCopy = Array.CreateInstance(targetType, column.Data.Length);
                for (var j = 0; j < column.Data.Length; j++)
                {
                    finalCopy.SetValue(Convert.ChangeType(column.Data.GetValue(j), targetType), j);
                }
            }
        }
        return (targetType, finalCopy);
    }
}
