using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;

namespace Database.Core.Operations;

public record HashJoinOperator(
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
        var count = 0;

        var ids = GetMatchingIdsAndCount(scanKeys, ref count);

        CopyColumnsToNewTable(rowGroup, ids, count, targetRg);

        return new RowGroup(count, targetRg, OutputColumnRefs);
    }

    private void CopyColumnsToNewTable(RowGroup rowGroup, RowRef?[] ids, int count, RowGroupRef targetRg)
    {
        // TODO need to ensure column ordering between the two tables
        for (var i = 0; i < ScanSource.ColumnRefs.Count; i++)
        {
            var columnRef = ScanSource.ColumnRefs[i];
            var sourceCol = BufferPool.GetColumn(columnRef with { RowGroup = rowGroup.RowGroupRef.RowGroup });

            var outputCol = OutputColumns[i];

            var filtered = FilterArray(sourceCol.ValuesArray, sourceCol.Type, ids, count);

            var column = ColumnHelper.CreateColumn(
                outputCol.ClrType,
                sourceCol.Name,
                filtered
            );
            BufferPool.WriteColumn(outputCol.ColumnRef, column, targetRg.RowGroup);
        }

        for (var i = 0; i < ProbeSource.ColumnRefs.Count; i++)
        {
            var outputCol = OutputColumns[i + ScanSource.ColumnRefs.Count];
            var columnRef = ProbeSource.ColumnRefs[i];
            var columnType = ProbeSource.Columns[i].ClrType;
            var values = Array.CreateInstance(columnType, count);

            var pos = 0;
            for (var j = 0; j < ids.Length; j++)
            {
                var rowId = ids[j];
                if (rowId == null)
                {
                    continue;
                }
                var sourceCol = BufferPool.GetColumn(columnRef with { RowGroup = rowId.Value.RowGroup.RowGroup });

                values.SetValue(sourceCol[rowId.Value.Row], pos);
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

    private RowRef?[] GetMatchingIdsAndCount(List<IColumn> scanKeys, ref int count)
    {
        var ids = _hashTable.Get(scanKeys);
        for (var i = 0; i < ids.Length; i++)
        {
            if (ids[i] != null)
            {
                count++;
            }
        }

        return ids;
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

        var hashTable = new HashTable<RowRef?>(ProbeKeys.Count);

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

            // TODO I need a hashtable with duplicates since keys may not be unique
            hashTable.Add(keys, rowArray);
            propRg = ProbeSource.Next();
        }

        return hashTable;
    }

    private Array FilterArray(Array source, Type type, RowRef?[] ids, int size)
    {
        var target = Array.CreateInstance(type, size);
        for (var i = 0; i < size; i++)
        {
            if (ids[i] == null)
            {
                continue;
            }
            target.SetValue(source.GetValue(i), i);
        }
        return target;
    }

    public override Cost EstimateCost()
    {
        var scanCost = ScanSource.EstimateCost();
        var probeCost = ProbeSource.EstimateCost();
        var outputRows = Math.Max(scanCost.OutputRows, probeCost.OutputRows); // TODO selectivity estimation/multiple
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
