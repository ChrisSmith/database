using System.Diagnostics;

namespace Database.Core.Expressions;

[DebuggerDisplay("{Expression} {Ascending ? \"ASC\" : \"DESC\"}")]
public record OrderingExpression(BaseExpression Expression, bool Ascending = true) : BaseExpression
{
    public override IEnumerable<BaseExpression> Children()
    {
        yield return Expression;
    }

    public override string ToString()
    {
        return $"{Expression} {(Ascending ? "ASC" : "DESC")}";
    }

    protected override BaseExpression WithChildren(IReadOnlyList<BaseExpression> newChildren)
    {
        if (newChildren.Count != 1)
        {
            throw new ArgumentException($"OrderingExpression expects 1 child but received {newChildren.Count}.");
        }
        return this with { Expression = newChildren[0] };
    }
}
