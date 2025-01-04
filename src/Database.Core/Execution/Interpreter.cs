using Database.Core.Planner;

namespace Database.Core.Execution;

public class Interpreter
{
    public IEnumerable<RowGroup> Execute(QueryPlan plan)
    {
        var operation = plan.Operation;
        var group = operation.Next();
        while(group != null)
        {
            yield return group;
            group = operation.Next();
        }
    }
}
