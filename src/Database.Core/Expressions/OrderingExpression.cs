using System.Diagnostics;

namespace Database.Core.Expressions;

[DebuggerDisplay("{Expression} {Ascending ? \"ASC\" : \"DESC\"}")]
public record OrderingExpression(BaseExpression Expression, bool Ascending = true) : BaseExpression
{
    public override IEnumerable<BaseExpression> Children()
    {
        yield return Expression;
    }

    public override string ToString()
    {
        return $"{Expression} {(Ascending ? "ASC" : "DESC")}";
    }
}
