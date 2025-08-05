namespace Database.Core;

// TODO limit can be a list of expressions?
// Needs offset also
public record LimitStatement(int Count) : IStatement;
