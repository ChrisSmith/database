using Database.Core.BufferPool;
using Database.Core.Catalog;

namespace Database.Core.Execution;

public interface IStorageLocation { }

public record struct MemoryStorage(TableId TableId) : IStorageLocation { }
public record struct ParquetStorage(ParquetFileHandle Handle) : IStorageLocation { }

public record struct ColumnRef(IStorageLocation Storage, int RowGroup, int Column);

public record struct RowGroupRef(int RowGroup);

public record struct RowRef(RowGroupRef RowGroup, int Row);
