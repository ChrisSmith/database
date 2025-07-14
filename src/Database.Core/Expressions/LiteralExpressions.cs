using System.Diagnostics;

namespace Database.Core.Expressions;

[DebuggerDisplay("{Literal}")]
public record NumericLiteral(double Literal) : BaseExpression
{

}

[DebuggerDisplay("{Literal}")]
public record StringLiteral(string Literal) : BaseExpression
{

}


[DebuggerDisplay("{Literal}")]
public record BoolLiteral(bool Literal) : BaseExpression
{

}

[DebuggerDisplay("null")]
public record NullLiteral() : BaseExpression
{

}
