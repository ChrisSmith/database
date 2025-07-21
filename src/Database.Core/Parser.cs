using System.Diagnostics.CodeAnalysis;
using System.Text.RegularExpressions;
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
            throw new ParseException(Peek(), "Unexpected token. Expected statement to be terminated");
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
            GroupByStatement? group = null;
            OrderByStatement? order = null;

            if (Match(WHERE))
            {
                where = ParseWhereStatement();
            }

            if (Match(GROUP))
            {
                group = ParseGroupByStatement();
            }

            if (Match(ORDER))
            {
                order = ParseOrderByStatement();
            }

            return new SelectStatement(selectList, from, where, group, order);
        }

        throw new ParseException(Peek(), "Expected statement");
    }

    private GroupByStatement ParseGroupByStatement()
    {
        Consume(BY, "Expected BY after GROUP");

        var expressions = new List<IExpression> { };
        while (!IsAtEnd())
        {
            expressions.Add(ParseExpr());

            if (Match(COMMA))
            {
                continue;
            }
            if (Check(HAVING, WINDOW, ORDER, LIMIT, UNION, INTERSECT, EXCEPT, SEMICOLON))
            {
                break;
            }

            throw new ParseException(Peek(), "Expected group by to terminate with one of [" +
                                             "having, window, order, limit, union, intersect, except, semicolon]");
        }

        return new GroupByStatement(expressions);
    }

    private OrderByStatement ParseOrderByStatement()
    {
        Consume(BY, "Expected BY");
        var expressions = new List<OrderingExpression> { };
        while (!IsAtEnd())
        {
            expressions.Add(ParseOrderingExpr());

            if (Match(COMMA))
            {
                continue;
            }
            if (Check(LIMIT, SEMICOLON))
            {
                break;
            }

            throw new ParseException(Peek(), "Expected order by to terminate with one of [limit, semicolon]");
        }

        return new OrderByStatement(expressions);
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

    private OrderingExpression ParseOrderingExpr()
    {
        var expr = ParseExpr();
        var ascending = true;
        if (Match(ASC)) { }
        else if (Match(DESC))
        {
            ascending = false;
        }

        if (Match(NULLS))
        {
            if (Match(FIRST))
            {
                // TODO store this
            }
            else
            {
                Consume(LAST, "Expected either FIRST or LAST after NULLS");
            }
        }

        return new OrderingExpression(expr, ascending);
    }

    private IExpression ParseExpr()
    {
        var expr = ParseOr();

        // https://www.sqlite.org/lang_expr.html
        // Operator precedence in sql
        // * / %
        // + -
        // & |
        // equality
        // not
        // and
        // or

        return expr;
    }

    private IExpression ParseOr()
    {
        var and = ParseAnd();
        if (Match(OR, out var token))
        {
            var right = ParseOr();
            return new BinaryExpression(token.TokenType, and, right);
        }

        return and;
    }

    private IExpression ParseAnd()
    {
        var not = ParseNot();
        if (Match(AND, out var token))
        {
            var right = ParseAnd();
            return new BinaryExpression(token.TokenType, not, right);
        }

        return not;
    }

    private IExpression ParseNot()
    {
        var equality = ParseEquality();
        if (Match(NOT, out var token))
        {
            var right = ParseNot();
            return new BinaryExpression(token.TokenType, equality, right);
        }

        return equality;
    }

    private IExpression ParseEquality()
    {
        var plus = ParsePlusMinus();
        if (Match(out var token, EQUAL, BANG_EQUAL, GREATER, GREATER_EQUAL, LESS, LESS_EQUAL, BETWEEN))
        {
            var right = ParseEquality();
            return new BinaryExpression(token.TokenType, plus, right);
        }

        return plus;
    }

    private IExpression ParsePlusMinus()
    {
        var plus = ParseMultiplication();
        if (Match(out var token, PLUS, MINUS))
        {
            var right = ParsePlusMinus();
            return new BinaryExpression(token.TokenType, plus, right);
        }

        return plus;
    }

    private IExpression ParseMultiplication()
    {
        var value = SingleParseExpr();
        if (Match(out var token, STAR, SLASH, PERCENT))
        {
            var right = ParseMultiplication();
            return new BinaryExpression(token.TokenType, value, right);
        }

        return value;
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
            if (num.Literal is int literal)
            {
                return new IntegerLiteral(literal);
            }
            return new DoubleLiteral((double)num.Literal!);
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

            if (ident.Lexeme == "date")
            {
                var token = Consume(STRING, "Expected string literal for date literal expression");
                // var date = DateOnly.Parse((string)token.Literal!);
                // return new DateLiteral(date);
                var date = DateTime.Parse((string)token.Literal!);
                return new DateTimeLiteral(date);
            }

            if (ident.Lexeme == "interval")
            {
                var token = Consume(STRING, "Expected string literal for interval literal expression");
                var unit = Consume(IDENTIFIER, "Expected unit for interval literal expression");

                var value = int.Parse((string)token.Literal!);
                var ts = unit.Lexeme.ToLower() switch
                {
                    // egh not great. I wonder what the spec says about this
                    "year" => TimeSpan.FromDays(value * 365),
                    "month" => TimeSpan.FromDays(value * 30),
                    "week" => TimeSpan.FromDays(value * 7),
                    "day" => TimeSpan.FromDays(value),
                    "hour" => TimeSpan.FromHours(value),
                    "minute" => TimeSpan.FromMinutes(value),
                    "second" => TimeSpan.FromSeconds(value),
                    _ => throw new ParseException(unit, $"Unknown interval unit: {unit.Lexeme}")
                };

                return new IntervalLiteral(ts);
            }


            // Column
            var column = ident.Lexeme;
            string? table = null;
            if (Match(DOT))
            {
                table = ident.Lexeme;
                column = Consume(IDENTIFIER, "Expected table name").Lexeme;
            }
            return new ColumnExpression(column, table) { Alias = column };
        }

        if (Match(LEFT_PAREN))
        {
            var inner = ParseExpr();
            // The grouping doesn't need a separate expr type, as the order
            // change happens at tree construction, instead of runtime
            Consume(RIGHT_PAREN, "Expected ')'");
            return inner;
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

    private bool Check(params TokenType[] tokenTypes)
    {
        if (IsAtEnd())
        {
            return false;
        }

        var token = Peek();
        foreach (var tokenType in tokenTypes)
        {
            if (token.TokenType == tokenType)
            {
                return true;
            }
        }
        return false;
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
