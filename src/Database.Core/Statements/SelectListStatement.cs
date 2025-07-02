using Database.Core.Expressions;

namespace Database.Core;

public record SelectListStatement(bool Distinct, List<IExpression> Expressions) : IStatement
{
}
