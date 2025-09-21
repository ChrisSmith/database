using Database.Core.Types;

namespace Database.Core.Catalog;

public enum DataType
{
    Unknown, // Used for late binding (subqueries)
    Int,
    Long,
    String,
    Float,
    Double,
    Decimal15,
    Decimal38,
    Date,
    DateTime,
    Interval,
    Bool,
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
        if (clrType == typeof(Decimal15))
        {
            return DataType.Decimal15;
        }
        if (clrType == typeof(Decimal38))
        {
            return DataType.Decimal38;
        }
        if (clrType == typeof(DateOnly))
        {
            return DataType.Date;
        }
        if (clrType == typeof(DateTime))
        {
            return DataType.DateTime;
        }
        if (clrType == typeof(TimeSpan))
        {
            return DataType.Interval;
        }
        if (clrType == typeof(bool))
        {
            return DataType.Bool;
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
            case DataType.Decimal15: return typeof(Decimal15);
            case DataType.Decimal38: return typeof(Decimal38);
            case DataType.Date: return typeof(DateOnly);
            case DataType.DateTime: return typeof(DateTime);
            case DataType.Interval: return typeof(TimeSpan);
            case DataType.Bool: return typeof(bool);
            default: throw new Exception($"No type mapping available from {dataType} to {typeof(DataType).FullName}");
        }
    }
}
