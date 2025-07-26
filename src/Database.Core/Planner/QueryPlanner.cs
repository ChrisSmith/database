using System.Diagnostics.Contracts;
using System.Net;
using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;
using Database.Core.Operations;

namespace Database.Core.Planner;

public class QueryPlanner(Catalog.Catalog catalog, ParquetPool bufferPool)
{
    private ExpressionBinder _binder = new(bufferPool, new FunctionRegistry());

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

        IReadOnlyList<ColumnSchema> inputColumns = table.Columns.Select(c => c).ToList();

        IOperation source = new FileScan(bufferPool, table.Location, catalog);

        select = ConstantFolding.Fold(select);

        var memRef = bufferPool.OpenMemoryTable();
        var memTable = bufferPool.GetMemoryTable(memRef.TableId);

        var expressions = _binder.Bind(select.SelectList.Expressions, inputColumns);

        (source, expressions, inputColumns) = MaterializeAdditionalColumns(memTable, inputColumns, expressions, source);

        if (select.Where != null)
        {
            (source, expressions, inputColumns) = FilterTable(select.Where, source, inputColumns, expressions);
        }

        // grouping
        var groupingExprs = select.Group?.Expressions ?? [];
        if (groupingExprs.Count > 0)
        {
            for (var i = 0; i < groupingExprs.Count; i++)
            {
                var expr = groupingExprs[i];
                _binder.Bind(expr, inputColumns);
            }
        }

        if (expressions.Any(ExpressionContainsAggregate) || groupingExprs.Count > 0)
        {
            BindAggregateExpressions(
                memRef,
                memTable,
                inputColumns,
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


        (source, expressions, inputColumns) = MaterializeProjection(source, inputColumns, expressions);
        if (select.SelectList.Distinct)
        {
            var distinct = new Distinct(source);
            return new QueryPlan(distinct);
        }

        return new QueryPlan(source);
    }

    private (IOperation, IReadOnlyList<BaseExpression>, IReadOnlyList<ColumnSchema>) MaterializeProjection(
        IOperation source,
        IReadOnlyList<ColumnSchema> inputColumns,
        IReadOnlyList<BaseExpression> expressions
        )
    {
        var memRef = bufferPool.OpenMemoryTable();
        var memTable = bufferPool.GetMemoryTable(memRef.TableId);

        // TODO the select column function isn't correctly bound here to the result of the filter table
        // expressions = _binder.Bind(expressions, inputColumns);

        var outputExpressions = new List<BaseExpression>(expressions.Count);
        var outputColumns = new List<ColumnSchema>(expressions.Count);
        for (var i = 0; i < expressions.Count; i++)
        {
            var expr = expressions[i];
            var newColumn = memTable.AddColumnToSchema(expr.Alias, expr.BoundFunction!.ReturnType);

            outputExpressions.Add(expr with
            {
                BoundOutputColumn = newColumn.ColumnRef,
            });
            outputColumns.Add(newColumn);
        }

        var op = new Projection(bufferPool, memTable, source, outputExpressions);
        return (op, outputExpressions, outputColumns);
    }

    private (IOperation, IReadOnlyList<BaseExpression>, IReadOnlyList<ColumnSchema>) MaterializeAdditionalColumns(
        MemoryBasedTable memTable,
        IReadOnlyList<ColumnSchema> inputColumns,
        IReadOnlyList<BaseExpression> expressions,
        IOperation source)
    {
        // If any projections require the computation of a new column, do it prior to the filters/aggregations
        // so that we can filter/aggregate on them too
        var outputColumns = new List<ColumnSchema>(inputColumns);
        var outputExpressions = new List<BaseExpression>(inputColumns.Count);
        var expressionsForEval = new List<BaseExpression>(expressions.Count);

        for (var i = 0; i < expressions.Count; i++)
        {
            var expr = _binder.Bind(expressions[i], inputColumns);
            if (expr is ColumnExpression c && c.Alias == c.Column)
            {
                // If this is a basic column expression without an alias,
                // we can reference the source directly
                outputExpressions.Add(expr);
                continue;
            }

            if (ExpressionContainsAggregate(expr))
            {
                // At this point we can't materialize the aggregate, so skip it
                outputExpressions.Add(expr);
                continue;
            }

            var column = memTable.AddColumnToSchema(expr.Alias, expr.BoundFunction!.ReturnType);
            expr = expr with
            {
                BoundOutputColumn = column.ColumnRef,
            };

            expressionsForEval.Add(expr);
            outputExpressions.Add(expr);
            outputColumns.Add(column);
        }

        if (expressionsForEval.Count == 0)
        {
            return (source, outputExpressions, inputColumns);
        }

        var op = new ProjectionBinaryEval(bufferPool, source, expressionsForEval);
        return (op, outputExpressions, outputColumns);
    }

    private (IOperation, IReadOnlyList<BaseExpression>, IReadOnlyList<ColumnSchema>) FilterTable(BaseExpression where, IOperation source,
        IReadOnlyList<ColumnSchema> inputColumns, IReadOnlyList<BaseExpression> expressions)
    {
        var whereExpr = _binder.Bind(where, inputColumns);
        if (whereExpr.BoundFunction is not BoolFunction predicate)
        {
            // TODO cast values to "truthy"
            throw new QueryPlanException($"Filter expression '{where}' is not a boolean expression");
        }

        var memRef = bufferPool.OpenMemoryTable();
        var memTable = bufferPool.GetMemoryTable(memRef.TableId);

        var outputColumns = new List<ColumnSchema>(inputColumns.Count);
        var outputColumnsRefs = new List<ColumnRef>(inputColumns.Count);
        for (var i = 0; i < inputColumns.Count; i++)
        {
            var existingColumn = inputColumns[i];
            var newColumn = memTable.AddColumnToSchema(existingColumn.Name, existingColumn.DataType);
            outputColumns.Add(newColumn);
            outputColumnsRefs.Add(newColumn.ColumnRef);
        }

        var outputExpressions = _binder.Bind(expressions, outputColumns);

        var op = new Filter(bufferPool, memTable, source, whereExpr, outputColumnsRefs);
        return (op, outputExpressions, outputColumns);
    }

    private List<BaseExpression> RemoveAggregatesFromExpressions(IReadOnlyList<BaseExpression> expressions)
    {
        // If an expression contains both an aggregate and binary expression, we must run the binary
        // expressions after computing the aggregates
        // So the table fed into the projection will be the result from the aggregation
        // rewrite the expressions to reference the resulting column instead of the agg function
        // Ie. count(Id) + 4 -> col[count] + 4

        BaseExpression ReplaceAggregate(BaseExpression expr)
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

                var args = new BaseExpression[f.Args.Length];
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

        var result = new List<BaseExpression>(expressions.Count);
        for (var i = 0; i < expressions.Count; i++)
        {
            // TODO I probably need to rebind some positional information?
            result.Add(ReplaceAggregate(expressions[i]));
        }

        return result;
    }

    private bool ExpressionContainsAggregate(BaseExpression expr)
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
        IReadOnlyList<ColumnSchema> allColumns,
        IReadOnlyList<BaseExpression> expressions,
        List<BaseExpression> groupingExpressions
        )
    {
        for (var i = 0; i < expressions.Count; i++)
        {
            var expr = expressions[i];
            //BindExpression(expr, allColumns);

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
}

public class QueryPlanException(string message) : Exception(message);
