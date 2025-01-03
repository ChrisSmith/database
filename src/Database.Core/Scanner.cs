using static Database.Core.TokenType;

namespace Database.Core;

public class Scanner
{
    private readonly string _source;

    private int _start, _current = 0;
    private int _line = 1;
    private int _column = 1;
    private readonly List<Token> _tokens = new List<Token>();
    
    private static readonly IReadOnlyDictionary<string, TokenType> Keywords = new Dictionary<string, TokenType>{
        {"select", SELECT},
        {"from", FROM},
        {"where", WHERE},
        {"having", HAVING},
        {"order", ORDER},
        {"by", BY},
        {"asc", ASC},
        {"desc", DESC},
        {"limit", LIMIT},
        {"offset", OFFSET},
        {"and", AND},
        {"or", OR},
    };
    
    public Scanner(string source)
    {
        _source = source;
    }

    public List<Token> ScanTokens()
    {
        while (!IsAtEnd())
        {
            // we are at the beginning of the next lexeme
            _start = _current;
            ScanToken();
        }

        _tokens.Add(new Token(EOF, "", null, _line, _column));
        return _tokens;
    }
    
    private void ScanToken()
    {
        var c = Advance();
        switch (c)
        {
            case '(': AddToken(LEFT_PAREN); break;
            case ')': AddToken(RIGHT_PAREN); break;
            case ',': AddToken(COMMA); break;
            case '.': AddToken(DOT); break;
            case '-': AddToken(MINUS); break;
            case '+': AddToken(PLUS); break;
            case ';': AddToken(SEMICOLON); break;
            case '*': AddToken(STAR); break;
            
            // case '!': addToken(match('=') ? BANG_EQUAL : BANG); break;
            case '=': AddToken(EQUAL); break;
            case '>': AddToken(Match('=') ? GREATER_EQUAL : GREATER); break;
            case '<': AddToken(Match('=') ? LESS_EQUAL : LESS); break;
            
            case ' ':
            case '\r':
            case '\t':
                // ignore whitespace
                break;
            case '\n':
                _line++;
                _column = 1;
                break;

            case '"': ParseString(); break;

            default:

                if (IsDigit(c)){
                    Number();
                } else if (IsAlpha(c)){
                    Identifier();
                } else {
                    throw new ParseException(_line, _column, $"unexpected character '{c}'.");
                }

                break;
        }
    }
    
    private bool IsAtEnd()
    {
        return _current >= _source.Length;
    }
    
    private char Peek() {
        if (IsAtEnd()){
            return '\0';
        }
        return _source[_current];
    }

    private char PeekNext() {
        if (_current +1 >= _source.Length){
            return '\0';
        }
        return _source[_current + 1];
    }

    private bool Match(char expected){
        if(IsAtEnd()){
            return false;
        }
        if (_source[_current] != expected){
            return false;
        }
        _current++;
        _column++;
        return true;
    }
    
    private void Number() {
        while(IsDigit(Peek())){
            Advance();
        }

        if (Peek() == '.' && IsDigit(PeekNext())){
            // consume .
            Advance();

            while(IsDigit(Peek())){
                Advance();
            }
        }

        AddToken(NUMBER, double.Parse(_source.SubstringPos(_start, _current)));
    }

    private void Identifier(){
        while(IsAlphaNumeric(Peek())){
            Advance();
        }

        var text = _source.SubstringPos(_start, _current);
            
        TokenType type;
        if (!Keywords.ContainsKey(text)){
            type = IDENTIFIER;
        } else {
            type = Keywords[text];
        }

        AddToken(type);
    }

    private bool IsAlphaNumeric(char c){
        return IsAlpha(c) || IsDigit(c);
    }
    private static bool IsAlpha(char c){
        return c >= 'a' && c <= 'z'
               || c >= 'A' && c <= 'Z'
               || c == '_';
    }

    private static bool IsDigit(char c){
        return c >= '0' && c <= '9';
    }
    private void ParseString() {
        while(Peek() != '"' && !IsAtEnd()) {
            if (Peek() == '\n') {
                _line++;
            }
            Advance();
        }

        if(IsAtEnd()){
            throw new ParseException(_line, _column, "Unterminated string.");
        }

        Advance(); // closing "

        // trim quotes
        var value = _source.SubstringPos(_start + 1, _current -1);
        AddToken(STRING, value);
    }

    private void AddToken(TokenType type, object? literal = null)
    {
        var text = _source.SubstringPos(_start, _current);
        var len = _current - _start;
        _tokens.Add(new Token(type, text, literal, _line, _column - len));
    }   

    private char Advance()
    {
        _column++;
        return _source[_current++];
    }
}
