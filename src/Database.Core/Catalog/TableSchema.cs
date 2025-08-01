using System.Diagnostics;
using Database.Core.BufferPool;
using Database.Core.Execution;

namespace Database.Core.Catalog;

public enum TableId : int { }

public record TableSchema(
    IStorageLocation Storage,
    TableId Id,
    string Name,
    IReadOnlyList<ColumnSchema> Columns,
    string Location,
    long NumRows,
    int NumRowGroups,
    List<RowGroupMeta> RowGroups
    )
{
    public RowGroup StatsRowGroup { get; set; }
    public MemoryStorage StatsTable { get; set; }
}

/// <summary>
///
/// </summary>
/// <param name="Statistics">per column statistics for this row group</param>
public record RowGroupMeta(long RowCount, List<Statistics> Statistics);

public record Statistics(
    long? NullCount,
    long? DistinctCount,
    object? MinValue,
    object? MaxValue
);

/// <summary>
/// Globally unique identifier for a given column
/// </summary>
public enum ColumnId : int { }

/// <summary>
/// Represents the schema for a column within a database table.
/// </summary>
[DebuggerDisplay("{Name} ({DataType}) {ColumnRef}")]
public record ColumnSchema(
    ColumnRef ColumnRef,
    ColumnId Id,
    string Name,
    DataType DataType,
    Type ClrType
    );
