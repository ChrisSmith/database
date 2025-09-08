using System.Diagnostics;
using Database.Core.Execution;

namespace Database.Core.Expressions;

[DebuggerDisplay("subquery({SubQueryId})")]
public record SubQueryResultExpression(int SubQueryId, bool Correlated) : BaseExpression
{
    /// <summary>
    /// Output memory table
    /// </summary>
    public MemoryStorage BoundMemoryTable { get; set; }

    /// <summary>
    /// Input memory table
    /// </summary>
    public MemoryStorage BoundInputMemoryTable { get; set; }

    public override IEnumerable<BaseExpression> Children()
    {
        yield break;
    }

    public override string ToString()
    {
        var prefix = Correlated ? "correlated-" : string.Empty;
        return $"{prefix}subquery({SubQueryId})";
    }

    protected override BaseExpression WithChildren(IReadOnlyList<BaseExpression> newChildren)
    {
        throw new NotSupportedException($"{GetType().Name} does not support replacing children.");
    }
}
