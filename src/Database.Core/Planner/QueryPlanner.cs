using System.Diagnostics.CodeAnalysis;
using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;

namespace Database.Core.Planner;

public class QueryPlanner(Catalog.Catalog catalog, ParquetPool bufferPool)
{
    private ExpressionBinder _binder = new(bufferPool, new FunctionRegistry());
    private PhysicalPlanner _physicalPlanner = new PhysicalPlanner(catalog, bufferPool);

    public LogicalPlan CreateLogicalPlan(IStatement statement)
    {
        if (statement is not SelectStatement select)
        {
            throw new QueryPlanException(
                $"Unknown statement type '{statement.GetType().Name}'. Cannot create query plan.");
        }

        select = ConstantFolding.Fold(select);
        select = QueryRewriter.ExpandStarStatements(select, catalog);

        var expressions = select.SelectList.Expressions;
        var plan = BindRelations(select);

        if (select.Where != null)
        {
            (var extendedSchema, expressions, var forEval) =
                ExtendedSchemaWithExpressions(plan.OutputSchema, expressions);

            if (forEval.Any())
            {
                plan = new Projection(plan, forEval, extendedSchema, AppendExpressions: true);
                expressions = _binder.Bind(expressions, plan.OutputSchema);
            }
            var where = _binder.Bind(select.Where, extendedSchema);
            plan = new Filter(plan, where, extendedSchema);
            expressions = _binder.Bind(expressions, plan.OutputSchema);
        }

        // Must bind here before ExpressionContainsAggregate so there are functions to check
        expressions = _binder.Bind(expressions, plan.OutputSchema);
        if (select.Group?.Expressions != null || expressions.Any(ExpressionContainsAggregate))
        {
            var groupingExprs = _binder.Bind(select.Group?.Expressions ?? [], plan.OutputSchema);
            // TODO if the groupings are not in the expressions, add them
            plan = new Aggregate(plan, groupingExprs, expressions, SchemaFromExpressions(expressions));
            expressions = RemoveAggregatesFromExpressions(expressions);
            expressions = _binder.Bind(expressions, plan.OutputSchema);
        }

        if (select.Order != null)
        {
            var orderBy = _binder.Bind(select.Order.Expressions, plan.OutputSchema);
            plan = new Sort(plan, orderBy, plan.OutputSchema);
            expressions = _binder.Bind(expressions, plan.OutputSchema);
        }

        expressions = _binder.Bind(expressions, plan.OutputSchema);
        plan = new Projection(plan, expressions, SchemaFromExpressions(expressions));

        if (select.SelectList.Distinct)
        {
            plan = new Distinct(plan, SchemaFromExpressions(expressions));
        }

        return plan;
    }

    private IReadOnlyList<ColumnSchema> SchemaFromExpressions(IReadOnlyList<BaseExpression> expressions)
    {
        var schema = new List<ColumnSchema>(expressions.Count);
        foreach (var expr in expressions)
        {
            schema.Add(new ColumnSchema(
                default,
                default,
                expr.Alias,
                expr.BoundFunction!.ReturnType,
                expr.BoundFunction!.ReturnType.ClrTypeFromDataType(),
                SourceTableName: "",
                SourceTableAlias: ""
                ));
        }

        return schema;
    }

    private LogicalPlan BindRelations(SelectStatement select)
    {
        var firstTable = (TableStatement)select.From.TableStatements.First();
        LogicalPlan plan = CreateScanForTable(firstTable);

        // TODO Projection Push Down
        // tableColumns = FilterToUsedColumns(select, tableColumns);

        if (select.From.TableStatements.Count > 1)
        {
            // let's see if they are helping us with the join type
            if (select.From.JoinStatements == null)
            {
                throw new QueryPlanException("Cross joins not supported yet");
            }
        }

        if (select.From.JoinStatements != null)
        {
            // create a left deep tree
            foreach (var join in select.From.JoinStatements)
            {
                var tableStmt = (TableStatement)join.Table;
                var right = CreateScanForTable(tableStmt);
                var extendedSchema = ExtendSchema(plan.OutputSchema, right.OutputSchema);
                plan = new Join(
                    plan,
                    right,
                    join.JoinType,
                    join.JoinConstraint,
                    extendedSchema
                    );
            }
        }

        return plan;

        Scan CreateScanForTable(TableStatement tableStmt)
        {
            var table = catalog.GetTable(tableStmt.Table);
            var tableAlias = tableStmt.Alias ?? "";
            var tableColumns = table.Columns.Select(c => c with
            {
                // Add the table alias here so the select from a join can disambiguate
                SourceTableAlias = tableAlias,
            }).ToList();

            return new Scan(
                table.Name,
                table.Id,
                select.Where,
                tableColumns,
                tableStmt.Alias);
        }
    }

    public QueryPlan CreatePlan(IStatement statement)
    {
        var logicalPlan = CreateLogicalPlan(statement);
        var physicalPlan = _physicalPlanner.CreatePhysicalPlan(logicalPlan);
        return new QueryPlan(physicalPlan);
    }

    private (
        List<ColumnSchema> outputColumns,
        List<BaseExpression> outputExpressions,
        List<BaseExpression> expressionsForEval
        ) ExtendedSchemaWithExpressions(
        IReadOnlyList<ColumnSchema> inputColumns,
        IReadOnlyList<BaseExpression> expressions)
    {
        // If any projections require the computation of a new column, do it prior to the filters/aggregations
        // so that we can filter/aggregate on them too
        var outputColumns = new List<ColumnSchema>(inputColumns);
        var expressionsForEval = new List<BaseExpression>(expressions.Count);
        var outputExpressions = new List<BaseExpression>(expressions.Count);

        for (var i = 0; i < expressions.Count; i++)
        {
            var expr = _binder.Bind(expressions[i], inputColumns);
            if (expr is ColumnExpression c && c.Alias == c.Column)
            {
                outputExpressions.Add(expr);
                continue;
            }

            if (ExpressionContainsAggregate(expr))
            {
                // At this point we can't materialize the aggregate, so skip it
                outputExpressions.Add(expr);
                continue;
            }

            var column = new ColumnSchema(
                default,
                default,
                expr.Alias,
                expr.BoundFunction!.ReturnType,
                expr.BoundFunction!.ReturnType.ClrTypeFromDataType(),
                SourceTableName: "",
                SourceTableAlias: ""
            );
            expr = expr with
            {
                // TODO binding just the output column here is kinda weird?
                // It might be better to rewerite the ast so this becomes a column
                // select in the rest of the pipeline
                BoundOutputColumn = column.ColumnRef,
            };
            outputExpressions.Add(expr);
            expressionsForEval.Add(expr);
            outputColumns.Add(column);
        }

        return (outputColumns, outputExpressions, expressionsForEval);
    }

    private IReadOnlyList<ColumnSchema> FilterToUsedColumns(SelectStatement select, IReadOnlyList<ColumnSchema> inputColumns)
    {
        var expressions = _binder.Bind(select.SelectList.Expressions, inputColumns);
        var usedColumns = new HashSet<ColumnRef>();

        foreach (var expr in expressions)
        {
            ExtractUsedColumns(expr);
        }

        if (select.Where != null)
        {
            ExtractUsedColumns(_binder.Bind(select.Where, inputColumns, ignoreMissingColumns: true));
        }
        if (select.Group?.Expressions != null)
        {
            var grouping = _binder.Bind(select.Group.Expressions, inputColumns);
            foreach (var expr in grouping)
            {
                ExtractUsedColumns(expr);
            }
        }
        if (select.Order?.Expressions != null)
        {
            var unwrapped = select.Order.Expressions.Select(e => e.Expression).ToList();
            var orderBy = _binder.Bind(unwrapped, inputColumns);
            foreach (var expr in orderBy)
            {
                ExtractUsedColumns(expr);
            }
        }

        var filtered = inputColumns.Where(c => usedColumns.Contains(c.ColumnRef)).ToList();
        return filtered;

        void ExtractUsedColumns(BaseExpression root)
        {
            root.Walk(expr =>
            {
                if (expr.BoundFunction is SelectFunction s)
                {
                    usedColumns.Add(s.ColumnRef);
                }
            });
        }
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

            if (expr is ColumnExpression or IntegerLiteral or DecimalLiteral or StringLiteral or BoolLiteral or NullLiteral)
            {
                return expr;
            }

            throw new QueryPlanException($"Expression {expr} is not supported when aggregates are present.");
        }

        var result = new List<BaseExpression>(expressions.Count);
        for (var i = 0; i < expressions.Count; i++)
        {
            result.Add(ReplaceAggregate(expressions[i]));
        }

        return result;
    }

    private bool ExpressionContainsAggregate(BaseExpression expr)
    {
        return expr.AnyChildOrSelf(e => e.BoundFunction is IAggregateFunction);
    }

    private IReadOnlyList<ColumnSchema> ExtendSchema(
        IReadOnlyList<ColumnSchema> left,
        IReadOnlyList<ColumnSchema> right
        )
    {
        // TODO what about duplicates?
        // I need a way to keep track of the source table to disambiguate
        // Do i/should I unbind anything here?
        var result = new List<ColumnSchema>(left.Count + right.Count);
        result.AddRange(left);
        result.AddRange(right);
        return result;
    }
}

public class QueryPlanException(string message) : Exception(message);
