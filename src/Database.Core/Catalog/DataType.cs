namespace Database.Core.Catalog;

public enum DataType
{
    Int,
    Long,
    String,
    Float,
    Double,
}

public static class DataTypeExtensions
{
    public static DataType DataTypeFromClrType(Type clrType)
    {
        if (clrType == typeof(string))
        {
            return DataType.String;
        }
        if (clrType == typeof(int))
        {
            return DataType.Int;
        }
        if (clrType == typeof(long))
        {
            return DataType.Long;
        }
        if (clrType == typeof(float))
        {
            return DataType.Float;
        }
        if (clrType == typeof(double))
        {
            return DataType.Double;
        }

        throw new Exception($"No type mapping available from {clrType.FullName} to {typeof(DataType).FullName}");
    }
}
