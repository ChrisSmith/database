using System.Diagnostics;

namespace Database.Core.Expressions;

[DebuggerDisplay("{Left} {Operator} {Right}")]
public record BinaryExpression(TokenType Operator, IExpression Left, IExpression Right) : BaseExpression
{

}
