using System.Diagnostics;
using Database.Core.Catalog;

namespace Database.Core.Expressions;

[DebuggerDisplay("cast({Expression} as {Type})")]
public record CastExpression(BaseExpression Expression, DataType Type) : BaseExpression(BoundDataType: Type)
{
    public override IEnumerable<BaseExpression> Children()
    {
        yield return Expression;
    }

    public override string ToString()
    {
        return $"cast({Expression} as {Type})";
    }
}
