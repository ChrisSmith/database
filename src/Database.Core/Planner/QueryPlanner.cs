using Database.Core.Catalog;
using Database.Core.Expressions;
using Database.Core.Functions;
using Database.Core.Operations;
using static Database.Core.TokenType;

namespace Database.Core.Planner;

public class QueryPlanner(Catalog.Catalog catalog)
{
    private FunctionRegistry _functions = new();

    public QueryPlan CreatePlan(IStatement statement)
    {
        if (statement is not SelectStatement select)
        {
            throw new QueryPlanException(
                $"Unknown statement type '{statement.GetType().Name}'. Cannot create query plan.");
        }

        var from = select.From;
        var table = catalog.Tables.FirstOrDefault(t => t.Name == from.Table);
        if (table == null)
        {
            throw new QueryPlanException($"Table '{from.Table}' not found in catalog.");
        }

        IOperation source = new FileScan(table.Location);


        // If any projections require the computation of a new column, do it prior to the filters/aggregations
        // so that we can filter/aggregate on them too
        var columns = new List<ColumnSchema>(table.Columns);
        var expressions = select.SelectList.Expressions;
        var expressionsForEval = new List<IExpression>(expressions.Count);

        for (var i = 0; i < expressions.Count; i++)
        {
            var expr = expressions[i];
            BindExpression(expr, table);

            if (expr is ColumnExpression c && c.Alias == c.Column)
            {
                // If this is a basic column expression without an alias, no need to add a new column to the projection
                // just use the source directly
                expr.BoundIndex = i;
                continue;
            }

            if (ExpressionContainsAggregate(expr))
            {
                // At this point we can't materialize the aggregate, so skip it
                continue;
            }

            expr.BoundIndex = columns.Count;

            if (expr is BinaryExpression { Alias: "" } b)
            {
                expr.Alias = b.Operator.ToString(); // TODO literal of the expression
            }

            columns.Add(new ColumnSchema((ColumnId)(-1), expr.Alias, expr.BoundFunction!.ReturnType, expr.BoundFunction!.ReturnType.ClrTypeFromDataType()));
            expressionsForEval.Add(expr);
        }
        table = new TableSchema((TableId)(-1), "temp", columns, "memory");
        source = new ProjectionBinaryEval(table, source, expressionsForEval);

        if (select.Where != null)
        {
            BindExpression(select.Where, table);
            if (select.Where.BoundFunction is not BoolFunction predicate)
            {
                // TODO cast values to "truthy"
                throw new QueryPlanException($"Filter expression '{select.Where}' is not a boolean expression");
            }
            source = new Filter(source, select.Where);
        }

        if (expressions.Any(ExpressionContainsAggregate))
        {
            table = BindAggregateExpressions(table, expressions);
            source = new Aggregate(source, expressions);
            expressions = RemoveAggregatesFromExpressions(expressions);
        }

        var projection = new Projection(table, source, expressions);
        if (select.SelectList.Distinct)
        {
            var distinct = new Distinct(projection);
            return new QueryPlan(distinct);
        }

        return new QueryPlan(projection);
    }

    private List<IExpression> RemoveAggregatesFromExpressions(List<IExpression> expressions)
    {
        // If an expression contains both an aggregate and binary expression, we must run the binary
        // expressions after computing the aggregates
        // So the table fed into the projection will be the result from the aggregation
        // rewrite the expressions to reference the resulting column instead of the agg function
        // Ie. count(Id) + 4 -> col[count] + 4

        IExpression ReplaceAggregate(IExpression expr)
        {
            if (expr is FunctionExpression f)
            {
                var boundFn = f.BoundFunction;
                if (boundFn is IAggregateFunction)
                {
                    return new ColumnExpression(f.Alias)
                    {
                        Alias = f.Alias,
                        BoundFunction = new SelectFunction(f.BoundIndex, f.BoundDataType!.Value),
                        BoundDataType = f.BoundDataType!.Value,
                    };
                }

                var args = new IExpression[f.Args.Length];
                for (var i = 0; i < args.Length; i++)
                {
                    args[i] = ReplaceAggregate(f.Args[i]);
                }

                return f with
                {
                    BoundFunction = boundFn,
                    Args = args,
                };
            }

            if (expr is BinaryExpression b)
            {
                return b with
                {
                    Left = ReplaceAggregate(b.Left),
                    Right = ReplaceAggregate(b.Right),
                };
            }

            if (expr is ColumnExpression or IntegerLiteral or DoubleLiteral or StringLiteral or BoolLiteral or NullLiteral)
            {
                return expr;
            }

            throw new QueryPlanException($"Expression {expr} is not supported when aggregates are present.");
        }

        var result = new List<IExpression>(expressions.Count);
        for (var i = 0; i < expressions.Count; i++)
        {
            // TODO I probably need to rebind some positional information?
            result.Add(ReplaceAggregate(expressions[i]));
        }

        return result;
    }

    private bool ExpressionContainsAggregate(IExpression expr)
    {
        // TODO need a generic way to traverse the tree, would clean this up a bit
        // return expr.Children.Any(ExpressionContainsAggregate)

        if (expr.BoundFunction is IAggregateFunction)
        {
            return true;
        }
        if (expr is BinaryExpression be)
        {
            return ExpressionContainsAggregate(be.Left) || ExpressionContainsAggregate(be.Right);
        }
        if (expr is FunctionExpression fn)
        {
            if (fn.Args.Any(ExpressionContainsAggregate))
            {
                return true;
            }
        }
        return false;
    }

    private TableSchema BindAggregateExpressions(TableSchema table, List<IExpression> expressions)
    {
        var columns = new List<ColumnSchema>();

        for (var i = 0; i < expressions.Count; i++)
        {
            var expr = expressions[i];
            expr.BoundIndex = i;
            BindExpression(expr, table);

            var expression = expressions[i];
            if (!ExpressionContainsAggregate(expr))
            {
                throw new QueryPlanException($"expression '{expression}' is not an aggregate function");
            }
            // TODO some in the grouping will be columns to hold constant

            columns.Add(new ColumnSchema((ColumnId)(-1), expr.Alias, expr.BoundFunction!.ReturnType, expr.BoundFunction!.ReturnType.ClrTypeFromDataType()));
        }

        return new TableSchema((TableId)(-1), "temp", columns, "memory");
    }

    private void BindExpression(IExpression expression, TableSchema table)
    {
        expression.BoundFunction = FunctionForExpression(expression, table);
        expression.BoundDataType = expression.BoundFunction.ReturnType;
    }

    private IFunction FunctionForExpression(IExpression expression, TableSchema table)
    {
        if (expression.BoundFunction != null)
        {
            return expression.BoundFunction;
        }

        if (expression is IntegerLiteral numInt)
        {
            return new LiteralFunction(-1, numInt.Literal, DataType.Int);
        }

        if (expression is DoubleLiteral num)
        {
            return new LiteralFunction(-1, num.Literal, DataType.Double);
        }

        if (expression is StringLiteral str)
        {
            return new LiteralFunction(-1, str.Literal, DataType.String);
        }

        if (expression is BoolLiteral b)
        {
            return new LiteralFunction(-1, b.Literal, DataType.Bool);
        }

        if (expression is ColumnExpression column)
        {
            var (_, index, colType) = FindColumnIndex(table, column);
            return new SelectFunction(index, colType!.Value);
        }

        if (expression is BinaryExpression be)
        {
            BindExpression(be.Left, table);
            BindExpression(be.Right, table);

            var left = be.Left;
            var right = be.Right;

            if (left.BoundDataType == null || left.BoundDataType != right.BoundDataType)
            {
                // TODO automatic type casts?
                throw new QueryPlanException(
                    $"left and right expression types do not match. got {left.BoundDataType} != {right.BoundDataType}");
            }

            var args = new[] { left, right };
            return (be.Operator) switch
            {
                EQUAL => _functions.BindFunction("=", args, table),
                GREATER => _functions.BindFunction(">", args, table),
                GREATER_EQUAL => _functions.BindFunction(">=", args, table),
                LESS => _functions.BindFunction("<", args, table),
                LESS_EQUAL => _functions.BindFunction("<=", args, table),
                STAR => _functions.BindFunction("*", args, table),
                PLUS => _functions.BindFunction("+", args, table),
                MINUS => _functions.BindFunction("-", args, table),
                SLASH => _functions.BindFunction("/", args, table),
                PERCENT => _functions.BindFunction("%", args, table),
                _ => throw new QueryPlanException($"operator '{be.Operator}' not setup for binding yet"),
            };
        }

        if (expression is FunctionExpression fn)
        {
            foreach (var arg in fn.Args)
            {
                BindExpression(arg, table);
            }
            return _functions.BindFunction(fn.Name, fn.Args, table);
        }

        throw new NotImplementedException($"unsupported expression type '{expression.GetType().Name}' for expression binding");
    }

    private (string, int, DataType?) FindColumnIndex(TableSchema table, IExpression exp)
    {
        if (exp.BoundIndex != -1)
        {
            return (exp.Alias, exp.BoundIndex, exp.BoundDataType);
        }

        // TODO we need to actually handle * and alias
        if (exp is ColumnExpression column)
        {
            var colIdx = table.Columns.FindIndex(c => c.Name == column.Column);
            if (colIdx == -1)
            {
                throw new QueryPlanException($"Column '{column.Column}' does not exist on table '{table.Name}'");
            }
            return (column.Column, colIdx, table.Columns[colIdx].DataType);
        }
        if (exp is FunctionExpression fun)
        {
            // Might need to eagerly bind functions so we have the datatypes
            return (fun.Name, -1, null); // function is bound to is position
        }
        if (exp is BinaryExpression b)
        {
            // probably want the literal text of the expression here to name the column
            return (b.Alias, b.BoundIndex, b.BoundDataType); // function is bound to is position
        }
        throw new QueryPlanException($"Unsupported expression type '{exp.GetType().Name}'");
    }
}

public class QueryPlanException(string message) : Exception(message);
