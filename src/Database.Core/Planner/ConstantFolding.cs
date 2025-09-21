using Database.Core.Expressions;
using static Database.Core.TokenType;

namespace Database.Core.Planner;

public static class ConstantFolding
{
    public static SelectStatement Simplify(SelectStatement select)
    {
        var newStatements = new List<BaseExpression>(select.SelectList.Expressions.Count);

        var expressions = select.SelectList.Expressions;
        foreach (var expr in expressions)
        {
            newStatements.Add(Simplify(expr));
        }

        var where = select.Where;
        if (where != null)
        {
            where = Simplify(where);
        }

        return select with
        {
            SelectList = select.SelectList with { Expressions = newStatements },
            Where = where,
        };
    }

    public static BaseExpression Simplify(BaseExpression expression)
    {
        List<Func<BaseExpression, BaseExpression>> rules = [Fold, SimplifyLikes];
        foreach (var rule in rules)
        {
            expression = rule(expression);
        }
        return expression;
    }

    private static BaseExpression Fold(BaseExpression expression)
    {
        return expression.Rewrite(expr =>
        {
            if (expr is BinaryExpression b)
            {
                var left = b.Left;
                var right = b.Right;

                if (left is IntegerLiteral li && right is IntegerLiteral ri)
                {
                    var result = b.Operator switch
                    {
                        PLUS => li.Literal + ri.Literal,
                        MINUS => li.Literal - ri.Literal,
                        STAR => li.Literal * ri.Literal,
                        SLASH => li.Literal / ri.Literal,
                        PERCENT => li.Literal % ri.Literal,
                        _ => throw new QueryPlanException($"Operator '{b.Operator}' not supported for constant folding")
                    };
                    return new IntegerLiteral(result);
                }
                if (left is Decimal15Literal ld && right is Decimal15Literal rd)
                {
                    var result = b.Operator switch
                    {
                        PLUS => ld.Literal + rd.Literal,
                        MINUS => ld.Literal - rd.Literal,
                        STAR => ld.Literal * rd.Literal,
                        SLASH => ld.Literal / rd.Literal,
                        PERCENT => ld.Literal % ld.Literal,
                        _ => throw new QueryPlanException($"Operator '{b.Operator}' not supported for constant folding")
                    };
                    return new Decimal15Literal(result);
                }
                if (left is Decimal38Literal ld38 && right is Decimal38Literal rd38)
                {
                    var result = b.Operator switch
                    {
                        PLUS => ld38.Literal + rd38.Literal,
                        MINUS => ld38.Literal - rd38.Literal,
                        STAR => ld38.Literal * rd38.Literal,
                        SLASH => ld38.Literal / rd38.Literal,
                        PERCENT => ld38.Literal % ld38.Literal,
                        _ => throw new QueryPlanException($"Operator '{b.Operator}' not supported for constant folding")
                    };
                    return new Decimal38Literal(result);
                }
                if (left is DateTimeLiteral ldt && right is IntervalLiteral ril)
                {
                    var result = b.Operator switch
                    {
                        PLUS => ril.Literal.Add(ldt.Literal),
                        MINUS => ril.Literal.Subtract(ldt.Literal),
                        _ => throw new QueryPlanException($"Operator '{b.Operator}' not supported for constant folding")
                    };
                    return new DateTimeLiteral(result);
                }
            }
            return expr;
        });
    }

    private static BaseExpression SimplifyLikes(BaseExpression expr)
    {
        return expr.Rewrite(e =>
        {
            if (e is BinaryExpression { Operator: LIKE, Left: { } col, Right: StringLiteral lit })
            {
                var firstWildcard = lit.Literal.IndexOf('%');
                var lastWildcard = lit.Literal.LastIndexOf('%');
                if (firstWildcard == 0 && firstWildcard == lastWildcard)
                {
                    return new FunctionExpression("ends_with",
                        col,
                        new StringLiteral(lit.Literal[1..^0]));
                }

                if (firstWildcard == lit.Literal.Length - 1 && firstWildcard == lastWildcard)
                {
                    return new FunctionExpression("starts_with",
                        col,
                        new StringLiteral(lit.Literal[0..^1]));
                }
            }

            return e;
        });
    }
}
