using System.Diagnostics.Contracts;
using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;
using static Database.Core.TokenType;

namespace Database.Core.Planner;

public class ExpressionBinder(ParquetPool bufferPool, FunctionRegistry functions)
{
    [Pure]
    public IReadOnlyList<BaseExpression> Bind(
        IReadOnlyList<BaseExpression> expressions,
        IReadOnlyList<ColumnSchema> columns
    )
    {
        var result = new List<BaseExpression>(expressions.Count);
        foreach (var expr in expressions)
        {
            result.Add(Bind(expr, columns));
        }
        return result;
    }

    [Pure]
    public BaseExpression Bind(
        BaseExpression expression,
        IReadOnlyList<ColumnSchema> columns
        )
    {
        var fun = FunctionForExpression(expression, columns);
        var alias = expression.Alias;
        if (expression is BinaryExpression b && alias == "")
        {
            alias = b.Operator.ToString(); // TODO literal of the expression
        }

        expression = expression with
        {
            BoundFunction = fun,
            BoundDataType = fun.ReturnType,
            Alias = alias,
        };

        if (expression is BinaryExpression be)
        {
            return be with
            {
                Left = Bind(be.Left, columns),
                Right = Bind(be.Right, columns),
            };
        }

        if (expression is FunctionExpression fn)
        {
            var boundArgs = new BaseExpression[fn.Args.Length];
            for (var i = 0; i < fn.Args.Length; i++)
            {
                boundArgs[i] = Bind(fn.Args[i], columns);
            }

            return fn with
            {
                Args = boundArgs,
            };
        }

        return expression;
    }

    [Pure]
    public IFunction FunctionForExpression(
        BaseExpression expression,
        IReadOnlyList<ColumnSchema> columns
        )
    {
        if (expression is IntegerLiteral numInt)
        {
            return new LiteralFunction(numInt.Literal, DataType.Int);
        }

        if (expression is DoubleLiteral num)
        {
            return new LiteralFunction(num.Literal, DataType.Double);
        }

        if (expression is StringLiteral str)
        {
            return new LiteralFunction(str.Literal, DataType.String);
        }

        if (expression is BoolLiteral b)
        {
            return new LiteralFunction(b.Literal, DataType.Bool);
        }

        if (expression is DateLiteral d)
        {
            return new LiteralFunction(d.Literal, DataType.Date);
        }

        if (expression is DateTimeLiteral dt)
        {
            return new LiteralFunction(dt.Literal, DataType.DateTime);
        }

        if (expression is IntervalLiteral il)
        {
            return new LiteralFunction(il.Literal, DataType.Interval);
        }

        if (expression is ColumnExpression column)
        {
            var (_, columnRef, colType) = FindColumnIndex(columns, column);
            return new SelectFunction(columnRef, colType!.Value, bufferPool);
        }

        if (expression is BinaryExpression be)
        {
            var left = Bind(be.Left, columns);
            var right = Bind(be.Right, columns);

            if (left.BoundDataType == null || left.BoundDataType != right.BoundDataType)
            {
                // TODO automatic type casts?
                throw new QueryPlanException(
                    $"left and right expression types do not match. got {left.BoundDataType} != {right.BoundDataType}");
            }

            var args = new[] { left, right };
            return (be.Operator) switch
            {
                EQUAL => functions.BindFunction("=", args),
                GREATER => functions.BindFunction(">", args),
                GREATER_EQUAL => functions.BindFunction(">=", args),
                LESS => functions.BindFunction("<", args),
                LESS_EQUAL => functions.BindFunction("<=", args),
                STAR => functions.BindFunction("*", args),
                PLUS => functions.BindFunction("+", args),
                MINUS => functions.BindFunction("-", args),
                SLASH => functions.BindFunction("/", args),
                PERCENT => functions.BindFunction("%", args),
                _ => throw new QueryPlanException($"operator '{be.Operator}' not setup for binding yet"),
            };
        }

        if (expression is FunctionExpression fn)
        {
            var boundArgs = new BaseExpression[fn.Args.Length];
            for (var i = 0; i < fn.Args.Length; i++)
            {
                boundArgs[i] = Bind(fn.Args[i], columns);
            }
            return functions.BindFunction(fn.Name, boundArgs);
        }

        if (expression is StarExpression)
        {
            // TODO this is a bit of a hack
            return new LiteralFunction(1, DataType.Int);
        }

        throw new NotImplementedException($"unsupported expression type '{expression.GetType().Name}' for expression binding");
    }

    private static (string, ColumnRef, DataType?) FindColumnIndex(IReadOnlyList<ColumnSchema> columns, BaseExpression exp)
    {
        // TODO we need to actually handle * and alias
        if (exp is ColumnExpression column)
        {
            var col = columns.SingleOrDefault(c => c.Name == column.Column);
            if (col == null)
            {
                throw new QueryPlanException($"Column '{column.Column}' does not exist on table");
            }

            return (column.Column, col.ColumnRef, col.DataType);
        }
        if (exp is FunctionExpression fun)
        {
            // Might need to eagerly bind functions so we have the datatypes
            return (fun.Name, default, null); // function is bound to is position
        }
        if (exp is BinaryExpression b)
        {
            // probably want the literal text of the expression here to name the column
            return (b.Alias, b.BoundOutputColumn, b.BoundDataType); // function is bound to is position
        }
        throw new QueryPlanException($"Unsupported expression type '{exp.GetType().Name}'");
    }

}
