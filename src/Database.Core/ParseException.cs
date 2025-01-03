namespace Database.Core;

public class ParseException : Exception
{
    public ParseException(int line, string message): this(line, "", message) { }
    
    public ParseException(int line, string where, string message): 
        base($"[line {line}] Error{where}: {message}") { }

    public ParseException(Token token, string message)
    : this(token.Line, 
        token.TokenType == TokenType.EOF 
            ? " at end" 
            : $" at '{token.Lexeme}'", 
        message) {}
}
