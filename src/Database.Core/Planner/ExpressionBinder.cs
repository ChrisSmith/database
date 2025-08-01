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
        IFunction? function = expression switch
        {
            IntegerLiteral numInt => new LiteralFunction(numInt.Literal, DataType.Int),
            DecimalLiteral num => new LiteralFunction(num.Literal, DataType.Decimal),
            StringLiteral str => new LiteralFunction(str.Literal, DataType.String),
            BoolLiteral b => new LiteralFunction(b.Literal, DataType.Bool),
            DateLiteral d => new LiteralFunction(d.Literal, DataType.Date),
            DateTimeLiteral dt => new LiteralFunction(dt.Literal, DataType.DateTime),
            IntervalLiteral il => new LiteralFunction(il.Literal, DataType.Interval),
            StarExpression => new LiteralFunction(1, DataType.Int), // TODO this is a bit of a hack
            _ => null,
        };

        if (function == null)
        {
            if (expression is ColumnExpression column)
            {
                var (_, columnRef, colType) = FindColumnIndex(columns, column, ignoreMissingColumns);
                function = new SelectFunction(columnRef, colType!.Value, bufferPool);
            }
            else if (expression is UnaryExpression unary)
            {
                var expr = Bind(unary.Expression, columns, ignoreMissingColumns);
                var args = new[] { expr };
                expression = unary with
                {
                    Expression = expr
                };
                function = (unary.Operator) switch
                {
                    NOT => functions.BindFunction("not", args),
                    _ => throw new QueryPlanException($"unary operator '{unary.Operator}' not setup for binding yet"),
                };
            }
            else if (expression is CastExpression cast)
            {
                var expr = Bind(cast.Expression, columns, ignoreMissingColumns);
                var args = new[] { expr };
                expression = cast with
                {
                    Expression = expr
                };
                function = functions.BindFunction(CastFnForType(cast.BoundDataType!.Value), args);
            }
            else if (expression is BinaryExpression be)
            {
                var left = Bind(be.Left, columns, ignoreMissingColumns);
                var right = Bind(be.Right, columns, ignoreMissingColumns);

                (left, right) = MakeCompatibleTypes(left, right, columns, ignoreMissingColumns);

                expression = be with
                {
                    Left = left,
                    Right = right,
                };

                var args = new[] { left, right };
                function = (be.Operator) switch
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
                    AND => functions.BindFunction("and", args),
                    OR => functions.BindFunction("or", args),
                    _ => throw new QueryPlanException($"operator '{be.Operator}' not setup for binding yet"),
                };
            }
            else if (expression is BetweenExpression bt)
            {
                var value = Bind(bt.Value, columns, ignoreMissingColumns);
                var lower = Bind(bt.Lower, columns, ignoreMissingColumns);
                var upper = Bind(bt.Upper, columns, ignoreMissingColumns);

                expression = bt with
                {
                    Value = value,
                    Lower = lower,
                    Upper = upper,
                };
                function = functions.BindFunction("between", [value, lower, upper]);
            }
            else if (expression is FunctionExpression fn)
            {
                var boundArgs = new BaseExpression[fn.Args.Length];
                for (var i = 0; i < fn.Args.Length; i++)
                {
                    boundArgs[i] = Bind(fn.Args[i], columns, ignoreMissingColumns);
                }

                expression = fn with
                {
                    Args = boundArgs,
                };
                function = functions.BindFunction(fn.Name, boundArgs);
            }
            else if (expression is OrderingExpression order)
            {
                var inner = Bind(order.Expression, columns, ignoreMissingColumns);
                function = inner.BoundFunction; // Is this ok?
                expression = order with
                {
                    Expression = inner,
                };
            }
            else
            {
                throw new NotImplementedException($"unsupported expression type '{expression.GetType().Name}' for expression binding");
            }
        }

        var alias = expression.Alias;
        if (alias == "")
        {
            alias = expression.ToString();
        }
        return expression with
        {
            BoundFunction = function,
            BoundDataType = function.ReturnType,
            Alias = alias,
        };
    }

    private (BaseExpression left, BaseExpression right) MakeCompatibleTypes(
        BaseExpression left,
        BaseExpression right,
        IReadOnlyList<ColumnSchema> columns,
        bool ignoreMissingColumns
        )
    {
        if (left.BoundDataType == null || right.BoundDataType == null)
        {
            throw new QueryPlanException(
                $"data type was not bound. {left.BoundDataType}, {right.BoundDataType}");
        }
        if (left.BoundDataType.Value == right.BoundDataType.Value)
        {
            return (left, right);
        }

        var leftType = left.BoundDataType.Value;
        var rightType = right.BoundDataType.Value;

        var integers = new[] { DataType.Int, DataType.Long };

        if (integers.Contains(leftType) && integers.Contains(rightType))
        {
            return (DoCast(left, DataType.Long), DoCast(right, DataType.Long));
        }

        if (integers.Contains(leftType) && rightType == DataType.Decimal)
        {
            return (DoCast(left, DataType.Decimal), right);
        }

        if (integers.Contains(rightType) && leftType == DataType.Decimal)
        {
            return (left, DoCast(right, leftType));
        }

        var floating = new[] { DataType.Float, DataType.Double };
        if (floating.Contains(leftType) && floating.Contains(rightType))
        {
            return (DoCast(left, DataType.Double), DoCast(right, DataType.Double));
        }

        if (ExpressionIsNonLiteralDecimal(left) || ExpressionIsNonLiteralDecimal(right))
        {
            return (DoCast(left, DataType.Decimal), DoCast(right, DataType.Decimal));
        }

        if (floating.Contains(leftType) && rightType == DataType.Decimal)
        {
            return (left, DoCast(right, leftType));
        }

        if (floating.Contains(rightType) && leftType == DataType.Decimal)
        {
            return (DoCast(left, rightType), right);
        }

        throw new QueryPlanException($"unable to automatically convert types '{leftType}' and '{rightType}' to a compatible type.");

        BaseExpression DoCast(BaseExpression expr, DataType targetType)
        {
            if (expr.BoundDataType.Value == targetType)
            {
                return expr;
            }

            return Bind(new CastExpression(expr, targetType)
            {
                Alias = expr.Alias,
            }, columns, ignoreMissingColumns);
        }

        bool ExpressionIsNonLiteralDecimal(BaseExpression expr)
        {
            return expr is not DecimalLiteral && expr.BoundDataType!.Value == DataType.Decimal;
        }
    }

    private string CastFnForType(DataType targetType)
    {
        return targetType switch
        {
            DataType.Int => "cast_int",
            DataType.Long => "cast_long",
            DataType.Float => "cast_float",
            DataType.Double => "cast_double",
            DataType.Decimal => "cast_decimal",
            _ => throw new QueryPlanException($"unsupported cast type '{targetType}'"),
        };
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
