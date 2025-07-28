using System.Diagnostics;

namespace Database.Core.Expressions;

[DebuggerDisplay("{Expression} {Ascending ? \"ASC\" : \"DESC\"}")]
public record OrderingExpression(BaseExpression Expression, bool Ascending = true) : BaseExpression
{

}
