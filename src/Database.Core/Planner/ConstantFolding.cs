using Database.Core.Expressions;
using static Database.Core.TokenType;

namespace Database.Core.Planner;

public static class ConstantFolding
{
    public static SelectStatement Fold(SelectStatement select)
    {
        var newStatements = new List<BaseExpression>(select.SelectList.Expressions.Count);

        var expressions = select.SelectList.Expressions;
        foreach (var expr in expressions)
        {
            newStatements.Add(Fold(expr));
        }

        var where = select.Where;
        if (where != null)
        {
            where = Fold(where);
        }

        return select with
        {
            SelectList = select.SelectList with { Expressions = newStatements },
            Where = where,
        };
    }

    public static BaseExpression Fold(BaseExpression expr)
    {
        if (expr is BinaryExpression b)
        {
            var left = Fold(b.Left);
            var right = Fold(b.Right);

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
            if (left is DecimalLiteral ld && right is DecimalLiteral rd)
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
                return new DecimalLiteral(result);
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

            return b with
            {
                Left = left,
                Right = right,
            };
        }

        if (expr is FunctionExpression f)
        {
            var args = new List<BaseExpression>(f.Args.Length);
            foreach (var arg in f.Args)
            {
                args.Add(Fold(arg));
            }
            return f with
            {
                Args = args.ToArray(),
            };
        }

        return expr;
    }
}
