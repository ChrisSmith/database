namespace Database.Core.Expressions;

public record BinaryExpression(Token Operator, IExpression Left, IExpression Right) : IExpression
{

}
