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
    public static DataType DataTypeFromClrType(this Type clrType)
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

    public static Type ClrTypeFromDataType(this DataType dataType)
    {
        switch (dataType)
        {
            case DataType.String: return typeof(string);
            case DataType.Int: return typeof(int);
            case DataType.Long: return typeof(long);
            case DataType.Float: return typeof(float);
            case DataType.Double: return typeof(double);
            default: throw new Exception($"No type mapping available from {dataType} to {typeof(DataType).FullName}");
        }
    }
}
