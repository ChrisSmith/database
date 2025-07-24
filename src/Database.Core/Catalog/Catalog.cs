using Database.Core.BufferPool;
using Parquet.Schema;

namespace Database.Core.Catalog;

public record Catalog(ParquetPool BufferPool)
{
    public List<TableSchema> Tables { get; } = new();

    private int _nextTableId = 0;
    private int _nextColumnId = 0;
    public void LoadTable(string name, string path)
    {
        var id = (TableId)(++_nextTableId);
        var handle = BufferPool.OpenFile(path);

        var schema = new List<ColumnSchema>(handle.DataFields.Length);
        foreach (var field in handle.DataFields)
        {
            var columnId = (ColumnId)(++_nextColumnId);
            schema.Add(new ColumnSchema(
                columnId,
                field.Name,
                field.ClrType.DataTypeFromClrType(),
                field.ClrType));
        }

        var table = new TableSchema(id, name, schema, path);
        Tables.Add(table);
    }
}
