using System.Diagnostics;
using Database.Core.Execution;

namespace Database.Core.Expressions;

[DebuggerDisplay("subquery({SubQueryId})")]
public record SubQueryResultExpression(int SubQueryId) : BaseExpression
{
    public MemoryStorage BoundMemoryTable { get; set; }

    public override IEnumerable<BaseExpression> Children()
    {
        yield break;
    }

    public override string ToString()
    {
        return $"subquery({SubQueryId})";
    }

    protected override BaseExpression WithChildren(IReadOnlyList<BaseExpression> newChildren)
    {
        throw new NotSupportedException($"{GetType().Name} does not support replacing children.");
    }
}
