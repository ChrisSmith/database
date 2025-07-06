namespace Database.Core.Catalog;

public record TableSchema(string Name, List<ColumnSchema> Columns, string Location)
{

}

public record ColumnSchema(string Name, DataType DataType, Type ClrType);
