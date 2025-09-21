using System.Diagnostics;
using System.Globalization;
using Database.Core.Types;

namespace Database.Core.Expressions;

public abstract record LiteralExpression : BaseExpression
{
    public override IEnumerable<BaseExpression> Children()
    {
        yield break;
    }

    protected override BaseExpression WithChildren(IReadOnlyList<BaseExpression> newChildren)
    {
        throw new NotSupportedException($"{GetType().Name} does not support replacing children.");
    }
}

[DebuggerDisplay("{Literal}")]
public record Decimal15Literal(Decimal15 Literal) : LiteralExpression
{
    public override string ToString()
    {
        return Literal.ToString(CultureInfo.InvariantCulture);
    }
}

[DebuggerDisplay("{Literal}")]
public record Decimal38Literal(Decimal38 Literal) : LiteralExpression
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
public record IntervalLiteral(Interval Literal) : LiteralExpression
{
    public override string ToString()
    {
        return Literal.ToString();
    }
}
