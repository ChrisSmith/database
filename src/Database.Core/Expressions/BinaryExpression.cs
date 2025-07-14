namespace Database.Core.Expressions;

public record BinaryExpression(TokenType Operator, IExpression Left, IExpression Right) : IExpression
{
    public int BoundIndex { get; set; } = -1;
}
