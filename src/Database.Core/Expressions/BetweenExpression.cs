using System.Diagnostics;

namespace Database.Core.Expressions;

[DebuggerDisplay("{Value} between {Lower} and {Upper}")]
public record BetweenExpression(BaseExpression Value, BaseExpression Lower, BaseExpression Upper, bool Negate) : BaseExpression
{
    public override IEnumerable<BaseExpression> Children()
    {
        yield return Value;
        yield return Lower;
        yield return Upper;
    }

    public override string ToString()
    {
        var prefix = Negate ? "not " : string.Empty;
        return $"{Value} {prefix}between {Lower} and {Upper}";
    }

    protected override BaseExpression WithChildren(IReadOnlyList<BaseExpression> newChildren)
    {
        if (newChildren.Count != 3)
        {
            throw new ArgumentException($"BetweenExpression expects 3 children but received {newChildren.Count}.");
        }

        return this with
        {
            Value = newChildren[0],
            Lower = newChildren[1],
            Upper = newChildren[2]
        };
    }
}
