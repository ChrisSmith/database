using Database.Core.Expressions;

namespace Database.Core;

public record SelectListStatement(bool Distinct, IReadOnlyList<BaseExpression> Expressions) : IStatement
{
}
