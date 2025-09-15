using System.Numerics;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Planner;

namespace Database.Core.Operations;

// TODO I wonder what the engine would look like with IEnumerable/AsyncEnumerable/Task support
// using yield like would would probably be pretty nice and allow for async/io
// and allow the usage of IDisposable
public interface IOperation
{
    IReadOnlyList<ColumnSchema> Columns { get; }

    IReadOnlyList<ColumnRef> ColumnRefs { get; }

    RowGroup? Next(CancellationToken token);

    Cost EstimateCost();
    void Reset();
}

public abstract record BaseOperation(
    IReadOnlyList<ColumnSchema> Columns,
    IReadOnlyList<ColumnRef> ColumnRefs,
    CostEstimate CostEstimate) : IOperation
{
    public abstract RowGroup? Next(CancellationToken token);

    public abstract void Reset();

    public abstract Cost EstimateCost();
}

public record Cost(
    BigInteger OutputRows,
    BigInteger CpuOperations,
    BigInteger DiskOperations,
    BigInteger TotalRowsProcessed,
    BigInteger TotalCpuOperations,
    BigInteger TotalDiskOperations)
{
    public Cost(BigInteger OutputRows, BigInteger CpuOperations, BigInteger DiskOperations)
    : this(OutputRows, CpuOperations, DiskOperations, BigInteger.Zero, BigInteger.Zero, BigInteger.Zero)
    {

    }

    public Cost Add(Cost newOp)
    {
        return newOp with
        {
            TotalRowsProcessed = TotalRowsProcessed + newOp.OutputRows + newOp.TotalRowsProcessed,
            TotalCpuOperations = TotalCpuOperations + newOp.CpuOperations + newOp.TotalCpuOperations,
            TotalDiskOperations = TotalDiskOperations + newOp.DiskOperations + newOp.TotalDiskOperations,
        };
    }

    public BigInteger TotalCost()
    {
        return TotalCpuOperations / 1000 + TotalDiskOperations / 10;
    }
}
