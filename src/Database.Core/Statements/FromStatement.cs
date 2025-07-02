namespace Database.Core;

public record FromStatement(string Table, string? Alias = null) : IStatement
{

}
