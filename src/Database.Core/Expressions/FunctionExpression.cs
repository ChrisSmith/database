namespace Database.Core.Expressions;

public record FunctionExpression(string Name, params IExpression[] Args) : IExpression;
