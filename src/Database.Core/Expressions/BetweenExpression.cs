using System.Diagnostics;

namespace Database.Core.Expressions;

[DebuggerDisplay("{Value} between {Lower} and {Upper}")]
public record BetweenExpression(BaseExpression Value, BaseExpression Lower, BaseExpression Upper) : BaseExpression
{
    public override IEnumerable<BaseExpression> Children()
    {
        yield return Value;
        yield return Lower;
        yield return Upper;
    }

    public override string ToString()
    {
        return $"{Value} between {Lower} and {Upper}";
    }
}
