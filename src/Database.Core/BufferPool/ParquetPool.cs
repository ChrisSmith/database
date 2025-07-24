using System.Collections.Concurrent;
using System.IO.MemoryMappedFiles;
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
