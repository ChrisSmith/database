using System.Diagnostics;

namespace Database.Core.Expressions;

[DebuggerDisplay("{Literal}")]
public record DoubleLiteral(double Literal) : BaseExpression
{

}

[DebuggerDisplay("{Literal}")]
public record IntegerLiteral(int Literal) : BaseExpression
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

[DebuggerDisplay("{Literal}")]
public record DateLiteral(DateOnly Literal) : BaseExpression
{

}

[DebuggerDisplay("{Literal}")]
public record DateTimeLiteral(DateTime Literal) : BaseExpression
{

}

[DebuggerDisplay("{Literal}")]
public record IntervalLiteral(TimeSpan Literal) : BaseExpression
{

}
