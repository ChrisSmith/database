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
        var reader = handle.Reader;
        var meta = reader.Metadata ?? throw new Exception("No metadata");

        var numColumns = handle.DataFields.Length;
        var schema = new List<ColumnSchema>(numColumns);
        for (var i = 0; i < numColumns; i++)
        {
            var field = handle.DataFields[i];
            var columnId = (ColumnId)(++_nextColumnId);

            schema.Add(new ColumnSchema(
                columnId,
                field.Name,
                field.ClrType.DataTypeFromClrType(),
                field.ClrType,
                i
                ));
        }

        var rowGroups = new List<RowGroupMeta>();
        for (var i = 0; i < reader.RowGroupCount; i++)
        {
            var stats = new List<Statistics>(numColumns);
            var rg = reader.RowGroups[0];
            for (var c = 0; c < numColumns; c++)
            {
                var field = handle.DataFields[c];
                var pstats = rg.GetStatistics(field) ?? throw new Exception($"No stats for {field.Name} in row group {i}");
                stats.Add(new Statistics(pstats.NullCount, pstats.DistinctCount, pstats.MinValue, pstats.MaxValue));
            }
            rowGroups.Add(new RowGroupMeta(rg.RowCount, stats));
        }

        var table = new TableSchema(
            id,
            name,
            schema,
            path,
            meta.NumRows,
            reader.RowGroupCount,
            rowGroups
            );
        Tables.Add(table);
    }
}
