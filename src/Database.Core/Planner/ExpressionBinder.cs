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
        IReadOnlyList<ColumnSchema> columns,
        bool ignoreMissingColumns = false
    )
    {
        var result = new List<BaseExpression>(expressions.Count);
        foreach (var expr in expressions)
        {
            result.Add(Bind(expr, columns, ignoreMissingColumns));
        }
        return result;
    }

    [Pure]
    public BaseExpression Bind(
        BaseExpression expression,
        IReadOnlyList<ColumnSchema> columns,
        bool ignoreMissingColumns = false
        )
    {
        var fun = FunctionForExpression(expression, columns, ignoreMissingColumns);
        var alias = expression.Alias;
        if (alias == "")
        {
            alias = expression.ToString();
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
                Left = Bind(be.Left, columns, ignoreMissingColumns),
                Right = Bind(be.Right, columns, ignoreMissingColumns),
            };
        }

        if (expression is BetweenExpression bt)
        {
            return bt with
            {
                Value = Bind(bt.Value, columns, ignoreMissingColumns),
                Lower = Bind(bt.Lower, columns, ignoreMissingColumns),
                Upper = Bind(bt.Upper, columns, ignoreMissingColumns),
            };
        }

        if (expression is FunctionExpression fn)
        {
            var boundArgs = new BaseExpression[fn.Args.Length];
            for (var i = 0; i < fn.Args.Length; i++)
            {
                boundArgs[i] = Bind(fn.Args[i], columns, ignoreMissingColumns);
            }

            return fn with
            {
                Args = boundArgs,
            };
        }

        return expression;
    }

    [Pure]
    private IFunction FunctionForExpression(
        BaseExpression expression,
        IReadOnlyList<ColumnSchema> columns,
        bool ignoreMissingColumns
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
            var (_, columnRef, colType) = FindColumnIndex(columns, column, ignoreMissingColumns);
            return new SelectFunction(columnRef, colType!.Value, bufferPool);
        }

        if (expression is BinaryExpression be)
        {
            var left = Bind(be.Left, columns, ignoreMissingColumns);
            var right = Bind(be.Right, columns, ignoreMissingColumns);

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

        if (expression is BetweenExpression bt)
        {
            var value = Bind(bt.Value, columns, ignoreMissingColumns);
            var lower = Bind(bt.Lower, columns, ignoreMissingColumns);
            var upper = Bind(bt.Upper, columns, ignoreMissingColumns);
            return functions.BindFunction("between", [value, lower, upper]);
        }

        if (expression is FunctionExpression fn)
        {
            var boundArgs = new BaseExpression[fn.Args.Length];
            for (var i = 0; i < fn.Args.Length; i++)
            {
                boundArgs[i] = Bind(fn.Args[i], columns, ignoreMissingColumns);
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

    private static (string, ColumnRef, DataType?) FindColumnIndex(
        IReadOnlyList<ColumnSchema> columns,
        BaseExpression exp,
        bool ignoreMissingColumns
        )
    {
        // TODO we need to actually handle * and alias
        if (exp is ColumnExpression column)
        {
            var col = columns.SingleOrDefault(c => c.Name == column.Column);
            if (col == null)
            {
                if (ignoreMissingColumns)
                {
                    return (column.Column, default, DataType.Int);
                }

                var columnNames = string.Join(", ", columns.Select(c => c.Name));
                throw new QueryPlanException($"Column '{column.Column}' was not found in list of available columns {columnNames}");
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
