namespace Database.Core.Expressions;

public record NumericLiteral(double Literal) : IExpression
{

}

public record StringLiteral(string Literal) : IExpression
{

}

public record BoolLiteral(bool Literal) : IExpression
{

}

public record NullLiteral() : IExpression
{

}
