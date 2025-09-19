using Database.Core.BufferPool;
using Database.Core.Execution;
using Database.Core.Types;
using Parquet.Schema;

namespace Database.Core.Catalog;

public record Catalog(ParquetPool BufferPool)
{
    public List<TableSchema> Tables { get; } = new();

    private static volatile int _nextTableId = 0;
    private static volatile int _nextColumnId = 0;
    private static volatile int _nextMemoryTableId = 0;

    public bool HasTable(string name)
    {
        var table = Tables.FirstOrDefault(t => t.Name == name);
        return table != null;
    }

    public TableSchema GetTable(string name)
    {
        var table = Tables.FirstOrDefault(t => t.Name == name);
        if (table == null)
        {
            throw new Exception($"Table '{name}' not found in catalog.");
        }
        return table;
    }

    public void LoadTable(string name, string path)
    {
        var id = (TableId)Interlocked.Increment(ref _nextTableId);
        var handle = BufferPool.OpenFile(path);
        var reader = handle.Reader;
        var meta = reader.Metadata ?? throw new Exception("No metadata");
        var tableRef = new ParquetStorage(id, handle);

        var numColumns = handle.DataFields.Length;
        var schema = new List<ColumnSchema>(numColumns);
        for (var i = 0; i < numColumns; i++)
        {
            var field = handle.DataFields[i];
            var columnId = NextColumnId();

            var columnRef = new ColumnRef(columnId, tableRef, -1, i);
            var type = TypeConversion.ConvertIfNecessary(field.ClrType);
            schema.Add(new ColumnSchema(
                columnRef,
                columnId,
                field.Name,
                type.DataTypeFromClrType(),
                type,
                SourceTableName: name,
                SourceTableAlias: ""
                ));
        }

        var rowGroups = new List<RowGroupMeta>();
        for (var i = 0; i < reader.RowGroupCount; i++)
        {
            var stats = new List<Statistics>(numColumns);
            var rg = reader.RowGroups[i];
            for (var c = 0; c < numColumns; c++)
            {
                var field = handle.DataFields[c];

                if (rg.GetMetadata(field) == null)
                {
                    // hmm whats up with the nation file?

                    stats.Add(new Statistics(0, int.MaxValue, int.MinValue, int.MaxValue));
                    continue;
                    // throw new Exception($"No metadata for {field.Name} in row group {i} of table {name}");
                }

                var pstats = rg.GetStatistics(field) ?? throw new Exception($"No stats for {field.Name} in row group {i}");
                stats.Add(new Statistics(pstats.NullCount, pstats.DistinctCount, pstats.MinValue, pstats.MaxValue));
            }
            rowGroups.Add(new RowGroupMeta(checked((int)rg.RowCount), stats));
        }

        var table = new TableSchema(
            tableRef,
            id,
            name,
            schema,
            path,
            meta.NumRows,
            reader.RowGroupCount,
            rowGroups
        );

        (table.StatsTable, table.StatsRowGroup) = BuildStatsTable(table);

        Tables.Add(table);
    }

    public (MemoryStorage, RowGroup) BuildStatsTable(TableSchema table)
    {
        var memRef = OpenMemoryTable();
        var memTable = BufferPool.GetMemoryTable(memRef.TableId);

        var statsRg = memTable.AddRowGroup();
        var rgCount = table.NumRowGroups;

        var outputRefs = new List<ColumnRef>();

        // Need to allocate the columns first
        for (var i = 0; i < table.Columns.Count; i++)
        {
            var column = table.Columns[i];
            var columnType = column.ClrType;
            var minColumn = memTable.AddColumnToSchema(column.Name + "_$min", column.DataType, table.Name, "");
            var maxColumn = memTable.AddColumnToSchema(column.Name + "_$max", column.DataType, table.Name, "");
            var distinctCountColumn = memTable.AddColumnToSchema(column.Name + "_$distinct_count", DataType.Int, table.Name, "");
            var nullCountColumn = memTable.AddColumnToSchema(column.Name + "_$null_count", DataType.Int, table.Name, "");
        }

        var statsColumns = memTable.Schema;

        for (var i = 0; i < table.Columns.Count; i++)
        {
            var column = table.Columns[i];
            var columnType = column.ClrType;
            if (columnType == typeof(decimal))
            {
                columnType = typeof(Decimal15);
            }

            var s = i * 4;
            var minColumn = statsColumns[s];
            var maxColumn = statsColumns[s + 1];
            var distinctCountColumn = statsColumns[s + 2];
            var nullCountColumn = statsColumns[s + 3];

            var minValues = Array.CreateInstance(columnType, rgCount);
            var maxValues = Array.CreateInstance(columnType, rgCount);
            var distinctCounts = new int[rgCount];
            var nullCounts = new int[rgCount];

            for (var j = 0; j < rgCount; j++)
            {
                var stats = table.RowGroups[j].Statistics;
                var columnStats = stats[i];

                minValues.SetValue(ChangeType(columnStats.MinValue, columnType), j);
                maxValues.SetValue(ChangeType(columnStats.MaxValue, columnType), j);
                distinctCounts[j] = (int)(columnStats.DistinctCount ?? int.MinValue);
                nullCounts[j] = (int)(columnStats.NullCount ?? int.MinValue);
            }

            WriteColumn(minColumn, minValues);
            WriteColumn(maxColumn, maxValues);
            WriteColumn(distinctCountColumn, distinctCounts);
            WriteColumn(nullCountColumn, nullCounts);
        }

        return (memRef, new RowGroup(
            rgCount,
            statsRg,
            outputRefs
        ));

        void WriteColumn(ColumnSchema columnSchema, Array values)
        {
            var col = ColumnHelper.CreateColumn(
                columnSchema.ClrType,
                columnSchema.Name,
                values
            );
            BufferPool.WriteColumn(columnSchema.ColumnRef, col, statsRg.RowGroup);
            outputRefs.Add(columnSchema.ColumnRef);
        }

        object? ChangeType(object? value, Type columnType)
        {
            if (columnType == typeof(DateTime) && value is int)
            {
                // The types are kinda off in our dataset here
                // These are just dates, not dattimes
                var asDouble = Convert.ToDouble(value);
                return DateTime.UnixEpoch.AddDays(asDouble);
            }

            if (columnType == typeof(Decimal15))
            {
                return value switch
                {
                    int i => new Decimal15(i),
                    long l => new Decimal15(l),
                    float f => new Decimal15(f),
                    double d => new Decimal15(d),
                    decimal d => new Decimal15(d),
                    _ => new Decimal15(Convert.ToDecimal(value)),
                };
            }

            return Convert.ChangeType(value, columnType);
        }
    }

    public TableSchema GetTable(TableId id)
    {
        return Tables.First(table => table.Id == id);
    }

    public TableSchema GetTableByPath(string path)
    {
        return Tables.First(table => table.Location == path);
    }

    public ColumnId NextColumnId()
    {
        return (ColumnId)Interlocked.Increment(ref _nextColumnId);
    }

    // TODO add some descritive info in here for debugging, like a name, how it was created
    public MemoryStorage OpenMemoryTable()
    {
        var id = (TableId)Interlocked.Decrement(ref _nextMemoryTableId);
        var storage = new MemoryStorage(id);
        BufferPool.PutMemoryTable(new MemoryBasedTable(storage, this));
        return storage;
    }
}
