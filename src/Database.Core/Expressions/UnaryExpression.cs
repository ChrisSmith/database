using System.Diagnostics;

namespace Database.Core.Expressions;

[DebuggerDisplay("{Operator} {Expression}")]
public record UnaryExpression(TokenType Operator, BaseExpression Expression) : BaseExpression
{
    public override IEnumerable<BaseExpression> Children()
    {
        yield return Expression;
    }

    public override string ToString()
    {
        return $"{Operator} {Expression}";
    }

    protected override BaseExpression WithChildren(IReadOnlyList<BaseExpression> newChildren)
    {
        if (newChildren.Count != 1)
        {
            throw new ArgumentException($"UnaryExpression expects 1 child but received {newChildren.Count}.");
        }
        return this with { Expression = newChildren[0] };
    }
}
