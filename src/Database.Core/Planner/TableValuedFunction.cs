using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Functions;

namespace Database.Core.Planner;

public record TableValuedFunction(
    DataType ReturnType,
    MemoryBasedTable Table
    ) : IFunction
{

}
