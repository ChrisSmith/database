using Database.Core.BufferPool;
using Database.Core.Planner;

namespace Database.Core.Execution;

public class Interpreter(ParquetPool BufferPool)
{
    public IEnumerable<MaterializedRowGroup> Execute(QueryPlan plan)
    {
        var operation = plan.Operation;
        var group = operation.Next();
        while (group != null)
        {
            yield return Materialize(group);
            group = operation.Next();
        }
    }

    private MaterializedRowGroup Materialize(RowGroup group)
    {
        var columns = new List<IColumn>(group.NumColumns);

        for (var i = 0; i < group.NumColumns; i++)
        {
            var columnDef = group.Columns[i];
            var column = BufferPool.GetColumn(columnDef with
            {
                RowGroup = group.RowGroupRef.RowGroup,
            });
            columns.Add(column);
        }

        var res = new MaterializedRowGroup(columns);
        return res;
    }
}
