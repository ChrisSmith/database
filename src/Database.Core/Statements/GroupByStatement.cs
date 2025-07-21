using Database.Core.Expressions;

namespace Database.Core;

public record GroupByStatement(List<IExpression> Expressions) : IStatement
{

}
