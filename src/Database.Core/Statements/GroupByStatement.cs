using Database.Core.Expressions;

namespace Database.Core;

public record GroupByStatement(List<BaseExpression> Expressions) : IStatement
{

}
