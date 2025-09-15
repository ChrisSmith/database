using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Planner;

namespace Database.Core.Operations;

public record SubqueryOperator(
    ParquetPool BufferPool,
    List<IOperation> UncorrelatedSources,
    List<IReadOnlyList<ColumnSchema>> UncorrelatedOutputSchemas,
    IOperation Source,
    CostEstimate CostEstimate
    ) : BaseOperation(Source.Columns, Source.ColumnRefs, CostEstimate)
{
    private bool _executedSubQueries;

    public override void Reset()
    {
        Source.Reset();
    }

    public override RowGroup? Next(CancellationToken token)
    {
        if (!_executedSubQueries)
        {
            for (var i = 0; i < UncorrelatedSources.Count; i++)
            {
                var subQuery = UncorrelatedSources[i];
                var sink = UncorrelatedOutputSchemas[i].Single();
                var table = BufferPool.GetMemoryTable(((MemoryStorage)sink.ColumnRef.Storage).TableId);

                RowGroup? next;
                do
                {
                    next = subQuery.Next(token);
                    if (next != null)
                    {
                        var sourceColumnRef = next.Columns.Single();
                        var column = BufferPool.GetColumn(sourceColumnRef with
                        {
                            RowGroup = next.RowGroupRef.RowGroup,
                        });

                        var rid = table.AddRowGroup().RowGroup;
                        BufferPool.WriteColumn(sink.ColumnRef, column, rid);
                    }
                } while (next != null);
            }

            _executedSubQueries = true;
        }

        return Source.Next(token);
    }

    public override Cost EstimateCost()
    {
        var sourceCost = Source.EstimateCost();
        foreach (var cost in UncorrelatedSources.Select(s => s.EstimateCost()))
        {
            sourceCost = sourceCost.Add(cost);
        }
        return sourceCost;
    }
}
