using System.Diagnostics;

namespace Database.Core.Expressions;

[DebuggerDisplay("{Column}")]
public record ColumnExpression(string Column, string? Table = null) : BaseExpression
{
    public override IEnumerable<BaseExpression> Children()
    {
        yield break;
    }

    public override string ToString()
    {
        return Column;
    }

    protected override BaseExpression WithChildren(IReadOnlyList<BaseExpression> newChildren)
    {
        throw new NotSupportedException($"{GetType().Name} does not support replacing children.");
    }
}
