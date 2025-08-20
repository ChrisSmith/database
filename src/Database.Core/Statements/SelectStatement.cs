using Database.Core.Expressions;

namespace Database.Core;

public record SelectStatement(
    SelectListStatement SelectList,
    FromStatement? From,
    BaseExpression? Where,
    GroupByStatement? Group,
    OrderByStatement? Order,
    LimitStatement? Limit,
    string? Alias
    ) : ITableStatement;
