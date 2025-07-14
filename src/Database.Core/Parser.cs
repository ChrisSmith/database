using System.Diagnostics.CodeAnalysis;
using Database.Core.Expressions;
using static Database.Core.TokenType;

namespace Database.Core;

public class Parser
{
    private readonly List<Token> _tokens;
    private int _current = 0;

    public Parser(List<Token> tokens)
    {
        _tokens = tokens;
    }

    public IStatement Parse()
    {
        var stmt = ParseStatement();

        while (!IsAtEnd() && Match(SEMICOLON))
        {
            // consume whitespace
        }

        if (!IsAtEnd())
        {
            throw new ParseException(Peek(), "Only one statement is allowed");
        }
        return stmt;
    }

    private IStatement ParseStatement()
    {
        if (Match(SELECT))
        {
            var selectList = ParseSelectListStatement();
            var from = ParseFromStatement();

            IExpression? where = null;
            IStatement? order = null;

            if (Match(WHERE))
            {
                where = ParseWhereStatement();
            }

            if (Match(ORDER))
            {
                order = ParseOrderByStatement();
            }

            return new SelectStatement(selectList, from, where, order);
        }

        throw new ParseException(Peek(), "Expected statement");
    }

    private IStatement ParseOrderByStatement()
    {
        Consume(BY, "Expected BY");
        // TODO identifier list
        var identifier = Consume(IDENTIFIER, "Expected column name");

        throw new NotImplementedException();
    }

    private IExpression ParseWhereStatement()
    {
        return ParseExpr();
    }

    private FromStatement ParseFromStatement()
    {
        var table = Consume(IDENTIFIER, "Expected table name");
        var tableName = table.Lexeme;

        // as is optional in an alias
        if (Match(AS) || Check(IDENTIFIER))
        {
            var alias = Consume(IDENTIFIER, "Expected alias").Lexeme;
            return new FromStatement(tableName, alias);
        }

        return new FromStatement(tableName);
    }

    private SelectListStatement ParseSelectListStatement()
    {
        var isDistinct = false;
        if (Match(ALL))
        {
            // all is a no-op
        }
        else
        {
            isDistinct = Match(DISTINCT);
        }

        var expressions = new List<IExpression> { };
        while (!IsAtEnd())
        {
            expressions.Add(ParseSelectExpression());

            if (Match(COMMA))
            {
                continue;
            }
            if (Match(FROM))
            {
                break;
            }

            throw new ParseException(Peek(), "Expected SELECT to terminate with FROM");
        }

        return new SelectListStatement(isDistinct, expressions);
    }

    // select * from table
    // select * from table where

    private IExpression ParseSelectExpression()
    {
        if (Match(STAR))
        {
            return new StarExpression();
        }

        // Table alias
        if (Check(IDENTIFIER)
            && Check(DOT, offset: 1)
            && Check(STAR, offset: 2))
        {
            var table = Consume(IDENTIFIER, "Expected column name or alias").Lexeme;
            Consume(DOT, "Expected .");
            Consume(STAR, "Expected *");
            return new StarExpression(table);
        }

        var expr = ParseExpr();

        if (Match(AS))
        {
            var alias = Consume(IDENTIFIER, "Expected alias").Lexeme;
            expr.Alias = alias;
        }

        return expr;
    }

    private IExpression ParseExpr()
    {
        var expr = SingleParseExpr();
        // TODO what about operator precedence
        // TODO needs to be all the operators in https://www.sqlite.org/lang_expr.html
        if (Match(out var token, EQUAL, BANG_EQUAL, GREATER, GREATER_EQUAL, LESS, LESS_EQUAL,
           PLUS, MINUS, STAR, SLASH, PERCENT, IS, NOT, BETWEEN
                ))
        {
            var right = ParseExpr();
            return new BinaryExpression(token.TokenType, expr, right);
        }

        return expr;
    }

    /**
     * A single literal, column, switch or function invocation
     */
    private IExpression SingleParseExpr()
    {
        // https://www.sqlite.org/syntax/expr.html

        // Literal
        if (Match(NUMBER, out var num))
        {
            return new NumericLiteral((double)num.Literal!);
        }
        if (Match(STRING, out var str))
        {
            return new StringLiteral((string)str.Literal!);
        }
        // TODO literal bools, null

        // TODO bound parameters (@foo)

        if (Match(IDENTIFIER, out var ident))
        {
            // function invocation
            if (Check(LEFT_PAREN))
            {
                var arguments = ParseFunctionArguments();
                return new FunctionExpression(ident.Lexeme, arguments);
            }

            // Column
            var column = ident.Lexeme;
            string? table = null;
            if (Match(DOT))
            {
                table = ident.Lexeme;
                column = Consume(IDENTIFIER, "Expected table name").Lexeme;
            }
            return new ColumnExpression(column, table);
        }

        throw new ParseException(Peek(), "Expected expression");
    }

    private IExpression[] ParseFunctionArguments()
    {
        // https://www.sqlite.org/syntax/function-arguments.html
        // TODO distinct + order by
        Consume(LEFT_PAREN, "Expected '('");
        if (Match(RIGHT_PAREN))
        {
            return [];
        }

        if (Match(STAR))
        {
            Consume(RIGHT_PAREN, "Expected ')'");
            return [new StarExpression()];
        }

        var arguments = new List<IExpression>();
        do
        {
            arguments.Add(ParseExpr());
        } while (Match(COMMA));

        Consume(RIGHT_PAREN, "Expected ')'");
        return arguments.ToArray();
    }


    /**
     * Conditionally consumes a token of a specific type
     */
    private bool Match(TokenType tokenType)
    {
        if (Peek().TokenType == tokenType)
        {
            Advance();
            return true;
        }
        return false;
    }

    private bool Match([NotNullWhen(true)] out Token? token, params TokenType[] tokenTypes)
    {
        token = null;
        foreach (var t in tokenTypes)
        {
            if (Peek().TokenType == t)
            {
                token = Advance();
                return true;
            }
        }

        return false;
    }

    /**
     * Conditionally consumes a token of a specific type
     */
    private bool Match(TokenType tokenType, [NotNullWhen(true)] out Token? token)
    {
        if (Peek().TokenType == tokenType)
        {
            token = Advance();
            return true;
        }

        token = null;
        return false;
    }

    /**
     * Used to consume a token of a specific type when it's the only valid option
     * Throws a ParseException if the token is not of the expected type
     */
    private Token Consume(TokenType tokenType, string message)
    {
        if (Check(tokenType))
        {
            return Advance();
        }
        throw new ParseException(Peek(), message);
    }

    /**
     * Used to check if the current token is of a specific type
     * Does not consume the token
     */
    private bool Check(TokenType tokenType, int offset = 0)
    {
        if (IsAtEnd(offset))
        {
            return false;
        }
        return Peek(offset).TokenType == tokenType;
    }

    private bool IsAtEnd(int offset = 0)
    {
        return Peek(offset).TokenType == EOF;
    }

    private Token Peek(int offset = 0)
    {
        return _tokens[_current + offset];
    }

    private Token Previous()
    {
        return _tokens[_current - 1];
    }

    private Token Advance()
    {
        if (!IsAtEnd())
        {
            _current++;
        }
        return Previous();
    }
}
