using Database.Core.Expressions;

namespace Database.Core;

public interface ITableStatement : IStatement
{

}

public record FromStatement(List<ITableStatement> TableStatements, List<JoinStatement>? JoinStatements = null) : IStatement
{

}

public record TableStatement(string Table, string? Alias = null) : ITableStatement { }


public enum JoinType
{
    Cross,
    Inner,
    Left,
    Right,
    Full,
    Semi,
}
public record JoinStatement(JoinType JoinType, ITableStatement Table, BaseExpression JoinConstraint) : IStatement;
