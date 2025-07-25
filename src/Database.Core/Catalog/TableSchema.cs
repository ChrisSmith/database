using Database.Core.Execution;

namespace Database.Core.Catalog;

public enum TableId : int { }

public record TableSchema(
    IStorageLocation Storage,
    TableId Id,
    string Name,
    List<ColumnSchema> Columns,
    string Location,
    long NumRows,
    int NumRowGroups,
    List<RowGroupMeta> RowGroups
    )
{

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
/// <param name="Index">The offset in the parquet file</param>
/// </summary>
public record ColumnSchema(
    ColumnRef ColumnRef,
    ColumnId Id,
    string Name,
    DataType DataType,
    Type ClrType,
    int Index
    );
