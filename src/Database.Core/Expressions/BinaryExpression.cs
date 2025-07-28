using System.Diagnostics;
using Database.Core.Functions;

namespace Database.Core.Expressions;

[DebuggerDisplay("{Left} {Operator} {Right}")]
public record BinaryExpression(TokenType Operator, BaseExpression Left, BaseExpression Right) : BaseExpression
{
    public override IEnumerable<BaseExpression> Children()
    {
        yield return Left;
        yield return Right;
    }
}
