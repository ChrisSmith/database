namespace Database.Core.Expressions;

public record AliasExpression(IExpression Expression, string Alias): IExpression
{
    
}
