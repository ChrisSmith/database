namespace Database.Core.Catalog;

public enum TableId : int { }

public record TableSchema(TableId Id, string Name, List<ColumnSchema> Columns, string Location)
{

}

public enum ColumnId : int { }

public record ColumnSchema(ColumnId Id, string Name, DataType DataType, Type ClrType);
