using System.Diagnostics;

namespace Database.Core.Expressions;

[DebuggerDisplay("{Expression} {Ascending ? \"ASC\" : \"DESC\"}")]
public record OrderingExpression(IExpression Expression, bool Ascending = true) : BaseExpression
{

}
