using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Expressions;
using Database.Core.Functions;
using Database.Core.Options;

namespace Database.Core.Planner;

public class QueryPlanner
{
    private ExpressionBinder _binder;
    private PhysicalPlanner _physicalPlanner;
    private CostBasedOptimizer _costBasedOptimizer;
    private QueryOptimizer _optimizer;
    private ConfigOptions _options;
    private readonly Catalog.Catalog _catalog;
    private readonly ParquetPool _bufferPool;

    public QueryPlanner(ConfigOptions options, Catalog.Catalog catalog, ParquetPool bufferPool)
    {
        _options = options;
        _catalog = catalog;
        _bufferPool = bufferPool;
        _binder = new ExpressionBinder(bufferPool, new FunctionRegistry());
        _optimizer = new QueryOptimizer(_options, _binder);
        _physicalPlanner = new PhysicalPlanner(_options, catalog, bufferPool);
        _costBasedOptimizer = new CostBasedOptimizer(_options, _physicalPlanner);
    }

    public LogicalPlan CreateLogicalPlan(IStatement statement, BindContext context)
    {
        if (statement is not SelectStatement select)
        {
            throw new QueryPlanException(
                $"Unknown statement type '{statement.GetType().Name}'. Cannot create query plan.");
        }

        select = ConstantFolding.Fold(select);
        select = QueryRewriter.ExpandStarStatements(select, _catalog);

        var expressions = select.SelectList.Expressions;
        var plan = BindRelations(context, select);

        if (select.Where != null)
        {
            // TODO I could probably do this at the logic rewrite stage
            // 1. duplicate any expression that is used by its alias in the where/groupby
            // 2. push down a projection to materialize it early, change the expression into a col ref
            (var extendedSchema, expressions, var forEval) =
                ExtendedSchemaWithExpressions(context, plan.OutputSchema, expressions);

            if (forEval.Any())
            {
                plan = new Projection(plan, forEval, extendedSchema, AppendExpressions: true);
                expressions = _binder.Bind(context, expressions, plan.OutputSchema);
            }
            var where = _binder.Bind(context, select.Where, extendedSchema);
            plan = new Filter(plan, where);
            expressions = _binder.Bind(context, expressions, plan.OutputSchema);
        }

        // Must bind here before ExpressionContainsAggregate so there are functions to check
        expressions = _binder.Bind(context, expressions, plan.OutputSchema);
        if (select.Group?.Expressions != null || expressions.Any(ExpressionContainsAggregate))
        {
            var groupingExprs = _binder.Bind(context, select.Group?.Expressions ?? [], plan.OutputSchema);
            // TODO if the groupings are not in the expressions, add them
            plan = new Aggregate(plan, groupingExprs, expressions);
            expressions = RemoveAggregatesFromExpressions(expressions);
            expressions = _binder.Bind(context, expressions, plan.OutputSchema);
        }

        if (select.Order != null)
        {
            var orderBy = _binder.Bind(context, select.Order.Expressions, plan.OutputSchema);
            plan = new Sort(plan, orderBy);
            expressions = _binder.Bind(context, expressions, plan.OutputSchema);
        }

        expressions = _binder.Bind(context, expressions, plan.OutputSchema);
        plan = new Projection(plan, expressions, SchemaFromExpressions(expressions));

        if (select.SelectList.Distinct)
        {
            plan = new Distinct(plan);
        }

        if (select.Limit != null)
        {
            plan = new Limit(plan, select.Limit.Count);
        }

        return plan;
    }

    public static IReadOnlyList<ColumnSchema> SchemaFromExpressions(IReadOnlyList<BaseExpression> expressions)
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

    private LogicalPlan BindRelations(BindContext context, SelectStatement select)
    {
        var allScans = new List<Scan>();
        foreach (var table in select.From.TableStatements)
        {
            allScans.Add(CreateScanForTable((TableStatement)table));
        }

        LogicalPlan plan = allScans.First();

        if (allScans.Count > 1)
        {
            foreach (var right in allScans.Skip(1))
            {
                plan = new Join(
                    plan,
                    right,
                    JoinType.Cross,
                    null
                );
            }
        }

        // let's see if they are helping us with the join type
        if (select.From.JoinStatements != null)
        {
            var finalSchema = plan.OutputSchema;

            var joinScans = new List<Tuple<JoinStatement, Scan>>();
            foreach (var join in select.From.JoinStatements)
            {
                var tableStmt = (TableStatement)join.Table;
                var scan = CreateScanForTable(tableStmt);
                finalSchema = ExtendSchema(finalSchema, scan.OutputSchema);
                joinScans.Add(new(join, scan));
            }

            foreach (var (joinStmt, right) in joinScans)
            {
                var constraint = _binder.Bind(context, joinStmt.JoinConstraint, finalSchema);
                plan = new Join(
                    plan,
                    right,
                    joinStmt.JoinType,
                    constraint
                );
            }
        }

        return plan;

        Scan CreateScanForTable(TableStatement tableStmt)
        {
            var table = _catalog.GetTable(tableStmt.Table);
            var tableAlias = tableStmt.Alias ?? "";
            var tableColumns = table.Columns.Select(c => c with
            {
                // Add the table alias here so the select from a join can disambiguate
                SourceTableAlias = tableAlias,
            }).ToList();

            context.AddSymbols(table, tableStmt.Alias);

            return new Scan(
                table.Name,
                table.Id,
                null,
                tableColumns,
                Projection: false,
                Alias: tableStmt.Alias);
        }
    }

    public QueryPlan CreatePlan(IStatement statement)
    {
        var context = new BindContext();
        var logicalPlan = CreateLogicalPlan(statement, context);
        logicalPlan = _optimizer.OptimizePlan(logicalPlan, context);
        var physicalPlan = _costBasedOptimizer.OptimizeAndLower(logicalPlan, context);
        return new QueryPlan(physicalPlan);
    }

    private (
        List<ColumnSchema> outputColumns,
        List<BaseExpression> outputExpressions,
        List<BaseExpression> expressionsForEval
        ) ExtendedSchemaWithExpressions(
        BindContext context,
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
            var expr = _binder.Bind(context, expressions[i], inputColumns);
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
                        BoundFunction = new SelectFunction(f.BoundOutputColumn, f.BoundDataType!.Value, _bufferPool),
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

    public static IReadOnlyList<ColumnSchema> ExtendSchema(
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
