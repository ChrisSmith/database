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
}
