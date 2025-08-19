using Database.Core.Expressions;

namespace Database.Core;

public record ExpressionList(IReadOnlyList<BaseExpression> Statements) : BaseExpression
{
    public override IEnumerable<BaseExpression> Children()
    {
        foreach (var stmt in Statements)
        {
            yield return stmt;
        }
    }

    protected override BaseExpression WithChildren(IReadOnlyList<BaseExpression> newChildren)
    {
        if (newChildren.Count != Statements.Count)
        {
            throw new Exception("Expected " + Statements.Count + " statements, got " + newChildren.Count);
        }
        return this with { Statements = newChildren };
    }
}
