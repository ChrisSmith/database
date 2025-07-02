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

            IStatement? where = null;
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

    private IStatement ParseWhereStatement()
    {
        throw new NotImplementedException();
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

    private IExpression ParseSelectExpression()
    {
        if (Match(STAR))
        {
            return new StarExpression();
        }

        string? column, table;

        var name = Consume(IDENTIFIER, "Expected column name or alias").Lexeme;

        if (Match(DOT))
        {
            table = name;

            if (Match(STAR))
            {
                return new StarExpression(table);
            }
            column = Consume(IDENTIFIER, "Expected column name").Lexeme;
        }
        else
        {
            column = name;
            table = null;
        }

        var expression = new ColumnExpression(column, table);

        if (Match(AS))
        {
            var alias = Consume(IDENTIFIER, "Expected alias").Lexeme;
            return new AliasExpression(expression, alias);
        }

        return expression;
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
    private bool Check(TokenType tokenType)
    {
        if (IsAtEnd())
        {
            return false;
        }
        return Peek().TokenType == tokenType;
    }

    private bool IsAtEnd()
    {
        return Peek().TokenType == EOF;
    }

    private Token Peek()
    {
        return _tokens[_current];
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
