namespace Database.Core;

public record Token(TokenType TokenType, string Lexeme, object? Literal, int Line);
