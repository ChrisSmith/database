using System.Diagnostics;
using Database.Core.Catalog;

namespace Database.Core.Expressions;

[DebuggerDisplay("cast({Expression} as {Type})")]
public record CastExpression(BaseExpression Expression, DataType Type) : BaseExpression(BoundDataType: Type)
{
    public override IEnumerable<BaseExpression> Children()
    {
        yield return Expression;
    }

    public override string ToString()
    {
        return $"cast({Expression} as {Type})";
    }

    protected override BaseExpression WithChildren(IReadOnlyList<BaseExpression> newChildren)
    {
        if (newChildren.Count != 1)
        {
            throw new ArgumentException($"CastExpression expects 1 child but received {newChildren.Count}.");
        }
        return this with { Expression = newChildren[0] };
    }
}
