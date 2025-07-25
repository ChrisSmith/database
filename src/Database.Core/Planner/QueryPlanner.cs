using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;
using Database.Core.Operations;
using static Database.Core.TokenType;

namespace Database.Core.Planner;

public class QueryPlanner(Catalog.Catalog catalog, ParquetPool bufferPool)
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

        var activeColumns = table.Columns.Select(c => c).ToList();

        IOperation source = new FileScan(bufferPool, table.Location, catalog);

        // Constant folding
        var expressions = select.SelectList.Expressions;
        for (var i = 0; i < expressions.Count; i++)
        {
            expressions[i] = FoldConstantExpressions(expressions[i]);
        }

        if (select.Where != null)
        {
            select = select with
            {
                Where = FoldConstantExpressions(select.Where),
            };
        }

        // If any projections require the computation of a new column, do it prior to the filters/aggregations
        // so that we can filter/aggregate on them too
        var memRef = bufferPool.OpenMemoryTable();
        var memTable = bufferPool.GetMemoryTable(memRef.TableId);
        var expressionsForEval = new List<IExpression>(expressions.Count);

        for (var i = 0; i < expressions.Count; i++)
        {
            var expr = expressions[i];
            BindExpression(expr, activeColumns);

            if (expr is ColumnExpression c && c.Alias == c.Column)
            {
                // If this is a basic column expression without an alias,
                // we can reference the source directly
                continue;
            }

            if (ExpressionContainsAggregate(expr))
            {
                // At this point we can't materialize the aggregate, so skip it
                continue;
            }

            if (expr is BinaryExpression { Alias: "" } b)
            {
                expr.Alias = b.Operator.ToString(); // TODO literal of the expression
            }

            var columnRef = memTable.AddColumnToSchema(expr.Alias, expr.BoundFunction!.ReturnType);
            expr.BoundOutputColumn = columnRef;

            // columns.Add(newColumn);
            expressionsForEval.Add(expr);
        }

        source = new ProjectionBinaryEval(bufferPool, source, expressionsForEval);

        // TODO the projection and filter here are schema changes
        // This causes the outputs of their operations to change location
        // How should I reflect that in the data model?
        // At a minimum I need to take the bindings for the expressions passed into the
        // things like projection, groupby and rebind them to their in memory copies
        // One option would be to make the expressions immutable

        if (select.Where != null)
        {
            BindExpression(select.Where, activeColumns);
            if (select.Where.BoundFunction is not BoolFunction predicate)
            {
                // TODO cast values to "truthy"
                throw new QueryPlanException($"Filter expression '{select.Where}' is not a boolean expression");
            }

            var outputFromFilter = new List<ColumnRef>(activeColumns.Count + expressionsForEval.Count);
            for (var i = 0; i < activeColumns.Count; i++)
            {
                var existingColumn = activeColumns[i];
                var newColumnRef = memTable.AddColumnToSchema(existingColumn.Name, existingColumn.DataType);
                outputFromFilter.Add(newColumnRef);
            }
            for (var i = 0; i < expressionsForEval.Count; i++)
            {
                var existingColumn = memTable.GetColumnSchema(expressionsForEval[i].BoundOutputColumn);
                var newColumnRef = memTable.AddColumnToSchema(existingColumn.Name, existingColumn.DataType);
                outputFromFilter.Add(newColumnRef);
            }

            source = new Filter(bufferPool, memTable, source, select.Where, outputFromFilter);
        }

        // grouping
        var groupingExprs = select.Group?.Expressions ?? [];
        if (groupingExprs.Count > 0)
        {
            for (var i = 0; i < groupingExprs.Count; i++)
            {
                var expr = groupingExprs[i];
                BindExpression(expr, activeColumns);
            }
        }

        if (expressions.Any(ExpressionContainsAggregate) || groupingExprs.Count > 0)
        {
            BindAggregateExpressions(
                memRef,
                memTable,
                activeColumns,
                expressions,
                groupingExprs);

            if (groupingExprs.Count > 0)
            {
                source = new HashAggregate(source, expressions, groupingExprs);
                expressions = RemoveAggregatesFromExpressions(expressions);
            }
            else
            {
                source = new Aggregate(source, expressions);
                expressions = RemoveAggregatesFromExpressions(expressions);
            }
        }

        if (select.Order != null)
        {
            source = new SortOperator(source, select.Order.Expressions);
        }

        var projection = new Projection(bufferPool, source, expressions);
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
                        BoundFunction = new SelectFunction(f.BoundOutputColumn, f.BoundDataType!.Value, bufferPool),
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

    private void BindAggregateExpressions(
        MemoryStorage memRef,
        MemoryBasedTable memTable,
        List<ColumnSchema> allColumns,
        List<IExpression> expressions,
        List<IExpression> groupingExpressions
        )
    {
        for (var i = 0; i < expressions.Count; i++)
        {
            var expr = expressions[i];
            // expr.BoundIndex = i;
            BindExpression(expr, allColumns);

            if (!ExpressionContainsAggregate(expr))
            {
                // If its an alias, see if its a grouping
                // If it is, update the bound index of the column
                var isGrouping = false;
                for (var j = 0; j < groupingExpressions.Count; j++)
                {
                    var gExpr = groupingExpressions[j];
                    if (gExpr.Alias == expr.Alias)
                    {
                        // TODO need to be recursive for everything in this expression?
                        // need to bind to a separate schema?
                        if (expr.BoundFunction is SelectFunction s)
                        {
                            // expr.BoundFunction = s with
                            // {
                            //     Index = j
                            // };
                        }
                        isGrouping = true;
                        break;
                    }
                }

                if (!isGrouping)
                {
                    throw new QueryPlanException($"expression '{expr}' is not an aggregate function");
                }
            }
            // TODO some in the grouping will be columns to hold constant

            memTable.AddColumnToSchema(expr.Alias, expr.BoundFunction!.ReturnType);
        }
    }

    private void BindExpression(IExpression expression, List<ColumnSchema> columns)
    {
        expression.BoundFunction = FunctionForExpression(expression, columns);
        expression.BoundDataType = expression.BoundFunction.ReturnType;
    }

    private IFunction FunctionForExpression(IExpression expression, List<ColumnSchema> columns)
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

        if (expression is DateLiteral d)
        {
            return new LiteralFunction(-1, d.Literal, DataType.Date);
        }

        if (expression is DateTimeLiteral dt)
        {
            return new LiteralFunction(-1, dt.Literal, DataType.DateTime);
        }

        if (expression is IntervalLiteral il)
        {
            return new LiteralFunction(-1, il.Literal, DataType.Interval);
        }

        if (expression is ColumnExpression column)
        {
            var (_, index, colType) = FindColumnIndex(columns, column);
            return new SelectFunction(index, colType!.Value, bufferPool);
        }

        if (expression is BinaryExpression be)
        {
            BindExpression(be.Left, columns);
            BindExpression(be.Right, columns);

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
                EQUAL => _functions.BindFunction("=", args),
                GREATER => _functions.BindFunction(">", args),
                GREATER_EQUAL => _functions.BindFunction(">=", args),
                LESS => _functions.BindFunction("<", args),
                LESS_EQUAL => _functions.BindFunction("<=", args),
                STAR => _functions.BindFunction("*", args),
                PLUS => _functions.BindFunction("+", args),
                MINUS => _functions.BindFunction("-", args),
                SLASH => _functions.BindFunction("/", args),
                PERCENT => _functions.BindFunction("%", args),
                _ => throw new QueryPlanException($"operator '{be.Operator}' not setup for binding yet"),
            };
        }

        if (expression is FunctionExpression fn)
        {
            foreach (var arg in fn.Args)
            {
                BindExpression(arg, columns);
            }
            return _functions.BindFunction(fn.Name, fn.Args);
        }

        if (expression is StarExpression)
        {
            // TODO this is a bit of a hack
            return new LiteralFunction(-1, 1, DataType.Int);
        }

        throw new NotImplementedException($"unsupported expression type '{expression.GetType().Name}' for expression binding");
    }

    private (string, ColumnRef, DataType?) FindColumnIndex(List<ColumnSchema> columns, IExpression exp)
    {
        if (exp.BoundOutputColumn != default)
        {
            return (exp.Alias, exp.BoundOutputColumn, exp.BoundDataType);
        }

        // TODO we need to actually handle * and alias
        if (exp is ColumnExpression column)
        {
            var colIdx = columns.FindIndex(c => c.Name == column.Column);
            if (colIdx == -1)
            {
                throw new QueryPlanException($"Column '{column.Column}' does not exist on table");
            }

            var columnRef = columns[colIdx].ColumnRef;
            return (column.Column, columnRef, columns[colIdx].DataType);
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

    private IExpression FoldConstantExpressions(IExpression expr)
    {
        if (expr is BinaryExpression b)
        {
            var left = FoldConstantExpressions(b.Left);
            var right = FoldConstantExpressions(b.Right);

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
            if (left is DoubleLiteral ld && right is DoubleLiteral rd)
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
                return new DoubleLiteral(result);
            }
            if (left is DateTimeLiteral ldt && right is IntervalLiteral ril)
            {
                var result = b.Operator switch
                {
                    PLUS => ldt.Literal + ril.Literal,
                    MINUS => ldt.Literal - ril.Literal,
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
            var args = new List<IExpression>(f.Args.Length);
            foreach (var arg in f.Args)
            {
                args.Add(FoldConstantExpressions(arg));
            }
            return f with
            {
                Args = args.ToArray(),
            };
        }

        return expr;
    }
}

public class QueryPlanException(string message) : Exception(message);
