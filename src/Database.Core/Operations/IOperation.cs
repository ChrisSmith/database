using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;

namespace Database.Core.Operations;

// TODO I wonder what the engine would look like with IEnumerable/AsyncEnumerable/Task support
// using yield like would would probably be pretty nice and allow for async/io
// and allow the usage of IDisposable
public interface IOperation
{
    IReadOnlyList<ColumnSchema> Columns { get; }

    IReadOnlyList<ColumnRef> ColumnRefs { get; }

    RowGroup? Next();

    Cost EstimateCost();
}

public abstract record BaseOperation(
    IReadOnlyList<ColumnSchema> Columns,
    IReadOnlyList<ColumnRef> ColumnRefs) : IOperation
{
    public abstract RowGroup? Next();

    public abstract Cost EstimateCost();
}

public record Cost(
    long OutputRows,
    long CpuOperations,
    long DiskOperations,
    long TotalRowsProcessed = 0,
    long TotalCpuOperations = 0,
    long TotalDiskOperations = 0)
{
    public Cost Add(Cost newOp)
    {
        return newOp with
        {
            TotalRowsProcessed = TotalRowsProcessed + newOp.OutputRows + newOp.TotalRowsProcessed,
            TotalCpuOperations = TotalCpuOperations + newOp.CpuOperations + newOp.TotalCpuOperations,
            TotalDiskOperations = TotalDiskOperations + newOp.DiskOperations + newOp.TotalDiskOperations,
        };
    }

    public long TotalCost()
    {
        const double CpuCost = .001;
        const double DiskCost = .1;

        return (long)(TotalCpuOperations * CpuCost + TotalDiskOperations * DiskCost);
    }
}
