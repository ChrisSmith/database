using System.Diagnostics.CodeAnalysis;
using Database.Core.Expressions;
using Database.Core.Types;
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

    public ParseResult Parse()
    {
        var explain = Match(EXPLAIN);
        var stmt = ParseStatement();

        while (!IsAtEnd() && Match(SEMICOLON))
        {
            // consume whitespace
        }

        if (!IsAtEnd())
        {
            throw new ParseException(Peek(), "Unexpected token. Expected statement to be terminated");
        }
        return new(stmt, explain);
    }

    private IStatement ParseStatement()
    {
        if (Match(SELECT))
        {
            var selectList = ParseSelectListStatement();
            var from = ParseFromStatement();

            BaseExpression? where = null;
            GroupByStatement? group = null;
            BaseExpression? having = null;
            OrderByStatement? order = null;
            LimitStatement? limit = null;

            if (Match(WHERE))
            {
                where = ParseWhereStatement();
            }

            if (Match(GROUP))
            {
                group = ParseGroupByStatement();
            }

            if (Match(HAVING))
            {
                having = ParseHavingStatement();
            }

            if (Match(ORDER))
            {
                order = ParseOrderByStatement();
            }

            if (Match(LIMIT))
            {
                limit = ParseLimit();
            }

            return new SelectStatement(
                selectList,
                from,
                where,
                group,
                having,
                order,
                limit,
                Alias: null);
        }

        throw new ParseException(Peek(), "Expected statement");
    }

    private BaseExpression ParseHavingStatement()
    {
        return ParseExpr();
    }

    private LimitStatement ParseLimit()
    {
        var num = Consume(NUMBER, "Expected number after LIMIT");
        return new LimitStatement((int)num.Literal);
    }

    private GroupByStatement ParseGroupByStatement()
    {
        Consume(BY, "Expected BY after GROUP");

        var expressions = new List<BaseExpression> { };
        while (!IsAtEnd())
        {
            expressions.Add(ParseExpr());

            if (Match(COMMA))
            {
                continue;
            }
            if (Check(HAVING, WINDOW, ORDER, LIMIT, UNION, INTERSECT, EXCEPT, SEMICOLON, RIGHT_PAREN))
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
            if (Check(LIMIT, SEMICOLON, RIGHT_PAREN))
            {
                break;
            }

            throw new ParseException(Peek(), "Expected order by to terminate with one of [limit, semicolon]");
        }

        return new OrderByStatement(expressions);
    }

    private BaseExpression ParseWhereStatement()
    {
        return ParseExpr();
    }

    private FromStatement ParseFromStatement()
    {

        var table = ParseTableOrSubquery();
        var tables = new List<ITableStatement> { table };

        if (Match(COMMA))
        {
            // from table1, table2 syntax
            do
            {
                table = ParseTableOrSubquery();
                tables.Add(table);
            } while (Match(COMMA));

            return new FromStatement(tables);
        }

        var joins = new List<JoinStatement>();
        // from table1 join table2 syntax
        while (Check(CROSS, JOIN, LEFT, RIGHT, FULL, INNER))
        {
            var joinType = ParseJoinType();
            var table2 = ParseTableOrSubquery();
            var constraint = ParseJoinConstraint();
            joins.Add(new JoinStatement(joinType, table2, constraint));
        }

        return new FromStatement(tables, joins);
    }

    private BaseExpression ParseJoinConstraint()
    {
        Consume(ON, "Expected ON before join constraint");
        return ParseExpr();
    }

    private JoinType ParseJoinType()
    {
        if (!Match(out var token, CROSS, JOIN, LEFT, RIGHT, FULL, INNER))
        {
            throw new ParseException(Peek(), "Expected join type");
        }

        switch (token.TokenType)
        {
            case CROSS:
                Consume(JOIN, "Expected JOIN after CROSS");
                return JoinType.Cross;
            case INNER:
                Consume(JOIN, "Expected JOIN after INNER");
                return JoinType.Inner;
            case JOIN:
                return JoinType.Inner;
            case LEFT:
                if (Match(OUTER))
                {
                    Consume(JOIN, "Expected JOIN after OUTER");
                }
                return JoinType.Left;
            case RIGHT:
                Match(OUTER);
                return JoinType.Right;
            case FULL:
                Match(OUTER);
                return JoinType.Full;
            default:
                throw new ParseException(token, "Expected join type");
        }
    }

    private ITableStatement ParseTableOrSubquery()
    {
        if (Match(IDENTIFIER, out var table))
        {
            var tableName = table.Lexeme;

            // as is optional in an alias
            if (Match(AS) || Check(IDENTIFIER))
            {
                var alias = Consume(IDENTIFIER, "Expected alias").Lexeme;
                return new TableStatement(tableName, alias);
            }

            return new TableStatement(tableName);
        }

        // Will need to add subquery support here
        // https://www.sqlite.org/syntax/table-or-subquery.html

        if (Match(LEFT_PAREN))
        {
            var nestedQuery = (SelectStatement)ParseStatement();
            Consume(RIGHT_PAREN, "Expected ')'");

            if (Match(AS) || Check(IDENTIFIER))
            {
                var alias = Consume(IDENTIFIER, "Expected alias").Lexeme;
                nestedQuery = nestedQuery with { Alias = alias };

                // column alias? not in sqlite but in Q13
                if (Match(LEFT_PAREN))
                {
                    var aliases = new List<string>();

                    do
                    {
                        aliases.Add(Consume(IDENTIFIER, "Expected column alias").Lexeme);
                    } while (Match(COMMA));
                    Consume(RIGHT_PAREN, "Expected ')'");

                    var nestedSelect = nestedQuery.SelectList;
                    if (nestedSelect.Expressions.Count != aliases.Count)
                    {
                        throw new ParseException(Peek(), $"Expected number of aliases to match number of columns. {nestedSelect.Expressions.Count} != {aliases.Count}");
                    }
                    var expressions = new List<BaseExpression>(nestedSelect.Expressions.Count);
                    for (var i = 0; i < nestedSelect.Expressions.Count; i++)
                    {
                        expressions.Add(nestedSelect.Expressions[i] with { Alias = aliases[i] });
                    }

                    return nestedQuery with { SelectList = nestedSelect with { Expressions = expressions } };
                }

                return nestedQuery;
            }

            return nestedQuery;
        }

        throw new ParseException(Peek(), "Expected table or subquery");
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

        var expressions = new List<BaseExpression> { };
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

    private BaseExpression ParseSelectExpression()
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
            return expr with { Alias = alias };
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

    public BaseExpression ParseExpr()
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

    private BaseExpression ParseOr()
    {
        var and = ParseAnd();
        if (Match(OR, out var token))
        {
            var right = ParseOr();
            return new BinaryExpression(token.TokenType, token.Lexeme, and, right);
        }

        return and;
    }

    private BaseExpression ParseAnd()
    {
        var not = ParseNot();
        if (Match(AND, out var token))
        {
            var right = ParseAnd();
            return new BinaryExpression(token.TokenType, token.Lexeme, not, right);
        }

        return not;
    }

    private BaseExpression ParseNot()
    {
        if (Match(NOT, out var token))
        {
            var right = ParseExpr();
            return new UnaryExpression(token.TokenType, right);
        }
        var equality = ParseEquality();
        return equality;
    }

    private BaseExpression ParseEquality()
    {
        var plus = ParsePlusMinus();
        // TODO do I need a peek here (just for IN, BETWEEN, LIKE)
        var negate = Match(NOT);

        if (Match(out var token, IN, EQUAL, BANG_EQUAL, GREATER, GREATER_EQUAL, LESS, LESS_EQUAL, LIKE))
        {
            if (negate)
            {
                if (token.TokenType is not (IN or LIKE))
                {
                    throw new ParseException(token, "Expected negation");
                }
                var right = ParseEquality();
                return new UnaryExpression(NOT,
                    new BinaryExpression(token.TokenType, token.Lexeme, plus, right));
            }
            else
            {
                var right = ParseEquality();
                return new BinaryExpression(token.TokenType, token.Lexeme, plus, right);
            }
        }

        if (Match(out token, BETWEEN))
        {
            var middle = ParseEquality();
            Consume(AND, "Expected AND after BETWEEN expression");
            var right = ParseEquality();
            return new BetweenExpression(plus, middle, right, negate);
        }

        if (negate)
        {
            throw new ParseException(Peek(), "Expected expression after NOT");
        }

        return plus;
    }

    private BaseExpression ParsePlusMinus()
    {
        var plus = ParseMultiplication();
        if (Match(out var token, PLUS, MINUS))
        {
            var right = ParsePlusMinus();
            return new BinaryExpression(token.TokenType, token.Lexeme, plus, right);
        }

        return plus;
    }

    private BaseExpression ParseMultiplication()
    {
        var value = SingleParseExpr();
        if (Match(out var token, STAR, SLASH, PERCENT))
        {
            var right = ParseMultiplication();
            return new BinaryExpression(token.TokenType, token.Lexeme, value, right);
        }

        return value;
    }

    /**
     * A single literal, column, switch or function invocation
     */
    private BaseExpression SingleParseExpr()
    {
        // https://www.sqlite.org/syntax/expr.html

        // Literal
        if (Match(NUMBER, out var num))
        {
            if (num.Literal is int literal)
            {
                return new IntegerLiteral(literal);
            }
            return new DecimalLiteral((decimal)num.Literal!);
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
                var functionName = ident.Lexeme;
                var (arguments, distinct) = ParseFunctionArguments();
                if (distinct)
                {
                    if (!functionName.Equals("count", StringComparison.OrdinalIgnoreCase))
                    {
                        throw new ParseException(Peek(), "DISTINCT is only supported on COUNT");
                    }
                    functionName = "count_distinct";
                }
                return new FunctionExpression(functionName, arguments);
            }

            if (ident.Lexeme.Equals("date", StringComparison.OrdinalIgnoreCase)) // TODO should this be a keyword?
            {
                var token = Consume(STRING, "Expected string literal for date literal expression");
                // var date = DateOnly.Parse((string)token.Literal!);
                // return new DateLiteral(date);
                var date = DateTime.Parse((string)token.Literal!);
                return new DateTimeLiteral(date);
            }

            if (ident.Lexeme.Equals("interval", StringComparison.OrdinalIgnoreCase))
            {
                var token = Consume(STRING, "Expected string literal for interval literal expression");
                var unit = Consume(IDENTIFIER, "Expected unit for interval literal expression");

                var value = int.Parse((string)token.Literal!);
                var ts = unit.Lexeme.ToLower() switch
                {
                    "year" => new Interval(IntervalType.Year, value),
                    "month" => new Interval(IntervalType.Month, value),
                    "week" => new Interval(IntervalType.Week, value),
                    "day" => new Interval(IntervalType.Day, value),
                    "hour" => new Interval(IntervalType.Hour, value),
                    "minute" => new Interval(IntervalType.Minute, value),
                    "second" => new Interval(IntervalType.Second, value),
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
            if (Check(SELECT))
            {
                var subquery = (SelectStatement)ParseStatement();
                Consume(RIGHT_PAREN, "Expected ')'");
                return new SubQueryExpression(subquery, ExistsOnly: false);
            }

            var inner = ParseExpr();

            // We might be in a list of expressions for the IN statement
            if (Match(COMMA))
            {
                var statements = new List<BaseExpression> { inner };
                do
                {
                    statements.Add(ParseExpr());
                } while (Match(COMMA));
                Consume(RIGHT_PAREN, "Expected ')'");

                return new ExpressionList(statements);
            }

            // The grouping doesn't need a separate expr type, as the order
            // change happens at tree construction, instead of runtime
            Consume(RIGHT_PAREN, "Expected ')'");
            return inner;
        }

        if (Match(CASE))
        {
            var caseExpression = ParseCaseExpression();
            Consume(END, "Expected END");
            return caseExpression;

        }

        if (Match(EXISTS))
        {
            Consume(LEFT_PAREN, "Expected '('");
            var subquery = (SelectStatement)ParseStatement();
            Consume(RIGHT_PAREN, "Expected ')'");
            return new SubQueryExpression(subquery, ExistsOnly: true);
        }

        throw new ParseException(Peek(), "Expected expression");
    }

    private CaseExpression ParseCaseExpression()
    {
        Consume(WHEN, "Expected WHEN");

        var conditions = new List<BaseExpression>();
        var results = new List<BaseExpression>();

        do
        {
            conditions.Add(ParseExpr());
            Consume(THEN, "Expected THEN");
            results.Add(ParseExpr());
        } while (Match(WHEN));

        if (Match(ELSE))
        {
            var result = ParseExpr();
            return new CaseExpression(conditions, results, result);
        }

        return new CaseExpression(conditions, results, null);
    }

    private (BaseExpression[], bool distinct) ParseFunctionArguments()
    {
        // https://www.sqlite.org/syntax/function-arguments.html
        // TODO distinct + order by
        Consume(LEFT_PAREN, "Expected '('");

        if (Match(RIGHT_PAREN))
        {
            return ([], false);
        }

        if (Match(STAR))
        {
            Consume(RIGHT_PAREN, "Expected ')'");
            return ([new StarExpression()], false);
        }

        var distinct = Match(DISTINCT);

        var arguments = new List<BaseExpression>();

        if (!Check(RIGHT_PAREN))
        {
            // peek ahead to see if this is
            // extract(year from col)
            if (Check(FROM, offset: 1))
            {
                if (Match(STRING, out var str))
                {
                    arguments.Add(new StringLiteral(str.Lexeme));
                }
                else
                {
                    // An identifier here isn't a column, it's a special context aware word
                    var ident = Consume(IDENTIFIER, "Expected identifier");
                    arguments.Add(new StringLiteral(ident.Lexeme));
                }
                Consume(FROM, "Expected FROM");
                arguments.Add(ParseExpr());
            }
            // TODO cast(x as y)
            else
            {
                // foo(a, b)
                do
                {
                    arguments.Add(ParseExpr());
                } while (Match(COMMA));
            }
        }

        Consume(RIGHT_PAREN, "Expected ')'");
        return (arguments.ToArray(), distinct);
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
