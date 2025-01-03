using Database.Core.Expressions;

namespace Database.Core;

public record SelectListStatement(List<IExpression> Expressions) : IStatement
{
}
