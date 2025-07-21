using Database.Core.Expressions;

namespace Database.Core;

public record OrderByStatement(List<OrderingExpression> Expressions) : IStatement
{

}
