using System.Collections.Concurrent;
using System.IO.MemoryMappedFiles;
using Database.Core.Catalog;
using Database.Core.Execution;
using Parquet;
using Parquet.Schema;

namespace Database.Core.BufferPool;

public record RefCounter<T>(T Value)
{
    private int _count;
    public void Increment() => Interlocked.Increment(ref _count);
    public void Decrement() => Interlocked.Decrement(ref _count);
}

public record ParquetFileHandle(string Path, ParquetReader Reader, DataField[] DataFields)
{

}

public class ParquetPool
{
    private ReaderWriterLockSlim _lock = new(LockRecursionPolicy.NoRecursion);
    private Dictionary<string, RefCounter<ParquetFileHandle>> _openFiles = new();

    private Dictionary<ColumnRef, IColumn> _columnCache = new();

    private int _nextMemoryTableId = 0;
    private Dictionary<TableId, MemoryBasedTable> _memoryTables = new();

    public ParquetFileHandle OpenFile(string path)
    {
        _lock.EnterReadLock();
        try
        {
            if (_openFiles.TryGetValue(path, out var counter))
            {
                counter.Increment();
                return counter.Value;
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }

        _lock.EnterWriteLock();
        try
        {
            if (_openFiles.TryGetValue(path, out var counter))
            {
                counter.Increment();
                return counter.Value;
            }
            else
            {
                // TODO instead of holding a global lock here we can have one per counter
                var reader = OpenReader(path);
                _openFiles.Add(path, reader);
                return reader.Value;
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public MemoryStorage OpenMemoryTable()
    {
        var id = (TableId)(--_nextMemoryTableId);
        var storage = new MemoryStorage(id);
        _memoryTables.Add(id, new MemoryBasedTable(storage));
        return storage;
    }

    public MemoryBasedTable GetMemoryTable(TableId id)
    {
        return _memoryTables[id];
    }

    public IColumn GetColumn(ColumnRef columnRef)
    {
        if (columnRef.Storage is MemoryStorage storage)
        {
            var table = _memoryTables[storage.TableId];
            return table.GetColumn(columnRef);
        }

        if (_columnCache.TryGetValue(columnRef, out var column))
        {
            return column;
        }
        if (columnRef.Storage is not ParquetStorage parquet)
        {
            throw new Exception($"unexpected storage type {columnRef.Storage.GetType().Name} for column ref");
        }
        if (columnRef.RowGroup < 0)
        {
            throw new Exception($"row group cannot be negative for parquet storage. {columnRef}");
        }

        var handle = parquet.Handle;
        var reader = handle.Reader.OpenRowGroupReader(columnRef.RowGroup);
        var field = handle.DataFields[columnRef.Column];
        var parquetCol = reader.ReadColumnAsync(field).GetAwaiter().GetResult();
        var (targetType, finalCopy) = TypeConversion.RemoveNullablesHack(parquetCol, field);
        var columnObj = ColumnHelper.CreateColumn(targetType, field.Name, columnRef.Column, finalCopy);
        _columnCache.Add(columnRef, columnObj);
        return columnObj;
    }

    public void WriteColumn(ColumnRef columnRef, IColumn column, int rowGroup)
    {
        if (columnRef == default)
        {
            throw new Exception("Column reference not set");
        }
        if (columnRef.Storage is not MemoryStorage memRef)
        {
            throw new Exception($"Column reference expected to be memory storage but got {columnRef.Storage}");
        }
        var table = GetMemoryTable(memRef.TableId);
        table.PutColumn(columnRef with
        {
            RowGroup = rowGroup,
        }, column);
    }

    // public T GetValue<T>(RowRef rowRef)
    // {
    //     var column = GetColumn(rowRef.ColumnRef);
    //     return (T)column[rowRef.Row]!;
    // }

    private RefCounter<ParquetFileHandle> OpenReader(string path)
    {
        var file = MemoryMappedFile.CreateFromFile(path);
        var reader = ParquetReader.CreateAsync(file.CreateViewStream()).GetAwaiter().GetResult();
        var schema = reader.Schema;
        var dataFields = schema.GetDataFields();
        var handle = new ParquetFileHandle(path, reader, dataFields);
        var counter = new RefCounter<ParquetFileHandle>(handle);
        counter.Increment();
        return counter;
    }
}
