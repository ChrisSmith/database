using Database.Core.Expressions;

namespace Database.Core;

internal record SelectStatement(
    SelectListStatement SelectList,
    FromStatement From,
    IExpression? Where,
    GroupByStatement? Group,
    OrderByStatement? Order) : IStatement;
