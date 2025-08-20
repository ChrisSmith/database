using System.Numerics;
using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;
using Database.Core.Planner;

namespace Database.Core.Operations;

public record HashJoinOperator(
    JoinType JoinType,
    ParquetPool BufferPool,
    IOperation ScanSource,
    IOperation ProbeSource,
    MemoryBasedTable Table,
    List<BaseExpression> ScanKeys,
    List<BaseExpression> ProbeKeys,
    List<ColumnSchema> OutputColumns,
    List<ColumnRef> OutputColumnRefs
) : BaseOperation(OutputColumns, OutputColumnRefs)
{
    private bool _done = false;
    private ExpressionInterpreter _interpreter = new ExpressionInterpreter();
    private HashTable<RowRef?>? _hashTable;

    public override RowGroup? Next()
    {
        if (_done)
        {
            return null;
        }

        if (_hashTable == null)
        {
            _hashTable = BuildHashTable();
        }

        var rowGroup = ScanSource.Next();
        if (rowGroup == null)
        {
            _done = true;
            return null;
        }

        var scanKeys = CalculateScanKeys(rowGroup);

        var targetRg = Table.AddRowGroup();

        if (JoinType == JoinType.Inner)
        {
            var (offsets, rowRefs) = PerformIdMatch(scanKeys);

            CopyColumnsToNewTable(rowGroup, offsets, rowRefs, targetRg);

            return new RowGroup(offsets.Count, targetRg, OutputColumnRefs);
        }
        if (JoinType == JoinType.Semi)
        {
            var contains = _hashTable!.Contains(scanKeys);
            var count = contains.Count(c => c);
            CopyColumnsToNewTable(rowGroup, contains, count, targetRg);

            return new RowGroup(count, targetRg, OutputColumnRefs);
        }

        throw new NotImplementedException($"Not implemented {JoinType}");
    }

    private void CopyColumnsToNewTable(
        RowGroup rowGroup,
        IReadOnlyList<bool> sourceRows,
        int count,
        RowGroupRef targetRg)
    {
        for (var i = 0; i < ScanSource.ColumnRefs.Count; i++)
        {
            var columnRef = ScanSource.ColumnRefs[i];
            var sourceCol = BufferPool.GetColumn(columnRef with { RowGroup = rowGroup.RowGroupRef.RowGroup });

            var outputCol = OutputColumns[i];

            if (sourceCol.Type != outputCol.ClrType)
            {
                throw new Exception($"Source Column({i}) {sourceCol.Name} is of type {sourceCol.Type} " +
                                    $"but output column {outputCol.Name} is of type {outputCol.ClrType}");
            }

            var values = Array.CreateInstance(sourceCol.Type, count);
            var k = 0;
            for (var j = 0; j < sourceRows.Count; j++)
            {
                if (sourceRows[j])
                {
                    values.SetValue(sourceCol[j], k);
                    k++;
                }
            }

            var column = ColumnHelper.CreateColumn(
                outputCol.ClrType,
                sourceCol.Name,
                values
            );
            BufferPool.WriteColumn(outputCol.ColumnRef, column, targetRg.RowGroup);
        }
    }

    private void CopyColumnsToNewTable(RowGroup rowGroup, IReadOnlyList<int> offsets, IReadOnlyList<RowRef?> rowRefs, RowGroupRef targetRg)
    {
        var count = offsets.Count;

        // TODO need to ensure column ordering between the two tables
        for (var i = 0; i < ScanSource.ColumnRefs.Count; i++)
        {
            var columnRef = ScanSource.ColumnRefs[i];
            var sourceCol = BufferPool.GetColumn(columnRef with { RowGroup = rowGroup.RowGroupRef.RowGroup });

            var outputCol = OutputColumns[i];

            if (sourceCol.Type != outputCol.ClrType)
            {
                throw new Exception($"Source Column({i}) {sourceCol.Name} is of type {sourceCol.Type} " +
                                    $"but output column {outputCol.Name} is of type {outputCol.ClrType}");
            }

            var values = Array.CreateInstance(sourceCol.Type, count);
            for (var j = 0; j < count; j++)
            {
                var rowId = offsets[j];
                values.SetValue(sourceCol[rowId], j);
            }

            var column = ColumnHelper.CreateColumn(
                outputCol.ClrType,
                sourceCol.Name,
                values
            );
            BufferPool.WriteColumn(outputCol.ColumnRef, column, targetRg.RowGroup);
        }

        for (var i = 0; i < ProbeSource.ColumnRefs.Count; i++)
        {
            var outputCol = OutputColumns[i + ScanSource.ColumnRefs.Count];
            var columnRef = ProbeSource.ColumnRefs[i];
            var probeColumn = ProbeSource.Columns[i];
            var columnType = probeColumn.ClrType;
            var values = Array.CreateInstance(columnType, count);

            if (probeColumn.ClrType != outputCol.ClrType)
            {
                throw new Exception($"Probe Column({i}) {probeColumn.Name} is of type {probeColumn.ClrType} " +
                                    $"but output column {outputCol.Name} is of type {outputCol.ClrType}");
            }

            var pos = 0;
            for (var j = 0; j < rowRefs.Count; j++)
            {
                var rowId = rowRefs[j]!.Value;
                var sourceCol = BufferPool.GetColumn(columnRef with { RowGroup = rowId.RowGroup.RowGroup });

                values.SetValue(sourceCol[rowId.Row], pos);
                pos++;
            }

            var column = ColumnHelper.CreateColumn(
                outputCol.ClrType,
                outputCol.Name,
                values
            );
            BufferPool.WriteColumn(outputCol.ColumnRef, column, targetRg.RowGroup);
        }
        // TODO loop around again till the batch is full?
        // Materializing here is probably slow, would be better to just use ids?
    }

    // Left side just return the offset into the current row group
    // Right side is RowRef
    private (List<int>, List<RowRef?>) PerformIdMatch(List<IColumn> scanKeys)
    {
        var (idx, ids) = _hashTable!.Get(scanKeys);
        return (idx, ids);
    }

    private List<IColumn> CalculateScanKeys(RowGroup rowGroup)
    {
        var scanKeys = new List<IColumn>(ScanKeys.Count);
        foreach (var key in ScanKeys)
        {
            var res = _interpreter.Execute(key, rowGroup);
            scanKeys.Add(res);
        }

        return scanKeys;
    }

    private HashTable<RowRef?> BuildHashTable()
    {
        if (ProbeKeys.Count != ScanKeys.Count)
        {
            throw new Exception("Probe and Scan keys must be the same length");
        }

        var cost = EstimateCost();
        var estimatedRows = checked((int)BigInteger.Max(BigInteger.Min(cost.OutputRows, int.MaxValue), 7));
        var keyTypes = ProbeKeys.Select(p => p.BoundDataType!.Value.ClrTypeFromDataType()).ToArray();
        var hashTable = new HashTable<RowRef?>(keyTypes, size: estimatedRows);

        var propRg = ProbeSource.Next();
        while (propRg != null)
        {
            var keys = new List<IColumn>(ProbeKeys.Count);
            foreach (var key in ProbeKeys)
            {
                var res = _interpreter.Execute(key, propRg);
                keys.Add(res);
            }

            var rowArray = new RowRef?[propRg.NumRows];
            for (var i = 0; i < propRg.NumRows; i++)
            {
                rowArray[i] = new RowRef(propRg.RowGroupRef, i);
            }

            hashTable.Add(keys, rowArray);
            propRg = ProbeSource.Next();
        }

        return hashTable;
    }

    public override Cost EstimateCost()
    {
        var scanCost = ScanSource.EstimateCost();
        var probeCost = ProbeSource.EstimateCost();
        var outputRows = BigInteger.Max(scanCost.OutputRows, probeCost.OutputRows); // TODO selectivity estimation/multiple
        var hashCreation = probeCost.OutputRows * ProbeKeys.Count * 2;

        return scanCost.Add(new Cost(
            OutputRows: outputRows,
            CpuOperations: scanCost.OutputRows * ProbeKeys.Count + hashCreation + probeCost.CpuOperations,
            DiskOperations: probeCost.DiskOperations,
            TotalCpuOperations: probeCost.TotalCpuOperations,
            TotalDiskOperations: probeCost.TotalDiskOperations,
            TotalRowsProcessed: probeCost.TotalRowsProcessed
        ));
    }
}
