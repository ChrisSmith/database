using Database.Core.Expressions;

namespace Database.Core;

public record SelectStatement(
    SelectListStatement SelectList,
    FromStatement? From,
    BaseExpression? Where,
    GroupByStatement? Group,
    BaseExpression? Having,
    OrderByStatement? Order,
    LimitStatement? Limit,
    string? Alias,
    CommonTableStatements? Ctes
    ) : ITableStatement;
