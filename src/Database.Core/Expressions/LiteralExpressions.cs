using System.Diagnostics;

namespace Database.Core.Expressions;

public abstract record LiteralExpression : BaseExpression
{
    public override IEnumerable<BaseExpression> Children()
    {
        yield break;
    }
}

[DebuggerDisplay("{Literal}")]
public record DoubleLiteral(double Literal) : LiteralExpression
{

}

[DebuggerDisplay("{Literal}")]
public record IntegerLiteral(int Literal) : LiteralExpression
{

}

[DebuggerDisplay("{Literal}")]
public record StringLiteral(string Literal) : LiteralExpression
{

}


[DebuggerDisplay("{Literal}")]
public record BoolLiteral(bool Literal) : LiteralExpression
{

}

[DebuggerDisplay("null")]
public record NullLiteral() : LiteralExpression
{

}

[DebuggerDisplay("{Literal}")]
public record DateLiteral(DateOnly Literal) : LiteralExpression
{

}

[DebuggerDisplay("{Literal}")]
public record DateTimeLiteral(DateTime Literal) : LiteralExpression
{

}

[DebuggerDisplay("{Literal}")]
public record IntervalLiteral(TimeSpan Literal) : LiteralExpression
{

}
