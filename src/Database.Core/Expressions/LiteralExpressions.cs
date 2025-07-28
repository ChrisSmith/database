using System.Diagnostics;
using System.Globalization;

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
    public override string ToString()
    {
        return Literal.ToString(CultureInfo.InvariantCulture);
    }
}

[DebuggerDisplay("{Literal}")]
public record IntegerLiteral(int Literal) : LiteralExpression
{
    public override string ToString()
    {
        return Literal.ToString(CultureInfo.InvariantCulture);
    }
}

[DebuggerDisplay("{Literal}")]
public record StringLiteral(string Literal) : LiteralExpression
{
    public override string ToString()
    {
        return Literal;
    }
}


[DebuggerDisplay("{Literal}")]
public record BoolLiteral(bool Literal) : LiteralExpression
{
    public override string ToString()
    {
        return Literal.ToString(CultureInfo.InvariantCulture);
    }
}

[DebuggerDisplay("null")]
public record NullLiteral() : LiteralExpression
{
    public override string ToString()
    {
        return "null";
    }
}

[DebuggerDisplay("{Literal}")]
public record DateLiteral(DateOnly Literal) : LiteralExpression
{
    public override string ToString()
    {
        return Literal.ToString(CultureInfo.InvariantCulture);
    }
}

[DebuggerDisplay("{Literal}")]
public record DateTimeLiteral(DateTime Literal) : LiteralExpression
{
    public override string ToString()
    {
        return Literal.ToString(CultureInfo.InvariantCulture);
    }
}

[DebuggerDisplay("{Literal}")]
public record IntervalLiteral(TimeSpan Literal) : LiteralExpression
{
    public override string ToString()
    {
        return Literal.ToString();
    }
}
