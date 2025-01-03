namespace Database.Core;

public class ParseException : Exception
{
    public ParseException(int line, int column, string message): this(line, column, "", message) { }
    
    public ParseException(int line, int column, string where, string message): 
        base($"[{line}:{column}] Error{where}: {message}") { }

    public ParseException(Token token, string message)
    : this(
        token.Line, 
        token.Column, 
        token.TokenType == TokenType.EOF 
            ? " at end" 
            : $" at '{token.Lexeme}'", 
        message
        ) {}
}
