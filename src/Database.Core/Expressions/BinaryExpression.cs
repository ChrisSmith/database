using System.Diagnostics;

namespace Database.Core.Expressions;

[DebuggerDisplay("{Left} {Operator} {Right}")]
public record BinaryExpression(TokenType Operator, string OperatorLiteral, BaseExpression Left, BaseExpression Right) : BaseExpression
{
    public override IEnumerable<BaseExpression> Children()
    {
        yield return Left;
        yield return Right;
    }

    public override string ToString()
    {
        return $"{Left} {OperatorLiteral} {Right}";
    }

    protected override BaseExpression WithChildren(IReadOnlyList<BaseExpression> newChildren)
    {
        if (newChildren.Count != 2)
        {
            throw new ArgumentException($"BinaryExpression expects 2 children but received {newChildren.Count}.");
        }
        return this with { Left = newChildren[0], Right = newChildren[1] };
    }
}
