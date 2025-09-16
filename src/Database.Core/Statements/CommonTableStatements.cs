namespace Database.Core;

public record CommonTableStatements(List<CommonTableStatement> TableStatements) : IStatement
{

}

public record CommonTableStatement(SelectStatement Statement) : IStatement
{

}
