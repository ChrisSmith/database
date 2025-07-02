namespace Database.Core.Catalog;

public record TableSchema(string Name, List<ColumnSchema> Columns, string Location)
{

}
