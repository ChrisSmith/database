namespace Database.Core.Expressions;

public record NumericLiteral(double Literal) : BaseExpression
{

}

public record StringLiteral(string Literal) : BaseExpression
{

}

public record BoolLiteral(bool Literal) : BaseExpression
{

}

public record NullLiteral() : BaseExpression
{

}
