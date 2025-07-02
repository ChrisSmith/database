namespace Database.Core.Expressions;

public record ColumnExpression(string Column, string? Table = null) : IExpression
{

}
