using System.Diagnostics.CodeAnalysis;
using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;
using Database.Core.Operations;
using Database.Core.Options;
using static Database.Core.TokenType;

namespace Database.Core.Planner;

public class PhysicalPlanner(ConfigOptions config, Catalog.Catalog catalog, ParquetPool bufferPool)
{
    private ExpressionBinder _binder = new(bufferPool, new FunctionRegistry());

    public IOperation CreatePhysicalPlan(LogicalPlan plan, BindContext context)
    {
        if (plan is Scan scan)
        {
            return CreateScan(scan, context);
        }
        if (plan is Filter filter)
        {
            var input = CreatePhysicalPlan(filter.Input, context);
            return CreateFilter(filter, input, context);
        }

        if (plan is Join join)
        {
            var left = CreatePhysicalPlan(join.Left, context);
            var right = CreatePhysicalPlan(join.Right, context);
            return CreateJoin(join, left, right, context);
        }

        if (plan is Aggregate aggregate)
        {
            var input = CreatePhysicalPlan(aggregate.Input, context);
            return CreateAggregate(aggregate, input, context);
        }

        if (plan is Projection project)
        {
            var input = CreatePhysicalPlan(project.Input, context);
            return CreateProjection(project, input, context);
        }

        if (plan is Distinct distinct)
        {
            var input = CreatePhysicalPlan(distinct.Input, context);
            return CreateDistinct(distinct, input, context);
        }

        if (plan is Sort sort)
        {
            var input = CreatePhysicalPlan(sort.Input, context);
            return CreateSort(sort, input, context);
        }

        if (plan is Limit limit)
        {
            var input = CreatePhysicalPlan(limit.Input, context);
            return CreateLimit(limit, input, context);
        }

        if (plan is TopNSort top)
        {
            var input = CreatePhysicalPlan(top.Input, context);
            return CreateTopNSort(top, input, context);
        }

        if (plan is Apply apply)
        {
            var correlatedPlans = new List<IOperation>();
            for (var i = 0; i < apply.Correlated.Count; i++)
            {
                var subquery = apply.Correlated[i];
                var subContext = subquery.BindContext ?? throw new QueryPlanException("Subquery has no bind context");
                subContext.SupportsLateBinding = false;
                var subPlan = CreatePhysicalPlan(subquery, subContext);
                correlatedPlans.Add(subPlan);
            }
            context.CorrelatedSubQueryOps.AddRange(correlatedPlans);

            var input = CreatePhysicalPlan(apply.Input, context);
            return input;
        }

        if (plan is PlanWithSubQueries planWithSub)
        {
            var unCorrelatedPlans = new List<IOperation>();
            for (var i = 0; i < planWithSub.Uncorrelated.Count; i++)
            {
                var subquery = planWithSub.Uncorrelated[i];
                var subContext = subquery.BindContext ?? throw new QueryPlanException("Subquery has no bind context");
                var subPlan = CreatePhysicalPlan(subquery, subContext);
                unCorrelatedPlans.Add(subPlan);
            }

            var main = CreatePhysicalPlan(planWithSub.Plan, context);
            return CreatePlanWithSubqueries(planWithSub, main, unCorrelatedPlans);
        }

        throw new NotImplementedException($"Type {plan.GetType()} is not supported in physical plan");
    }

    private IOperation CreateTopNSort(TopNSort top, IOperation input, BindContext context)
    {
        var inputColumns = input.Columns;

        var memRef = catalog.OpenMemoryTable();
        var memTable = bufferPool.GetMemoryTable(memRef.TableId);

        var outputColumns = new List<ColumnSchema>(inputColumns.Count);
        var outputColumnsRefs = new List<ColumnRef>(inputColumns.Count);
        for (var i = 0; i < inputColumns.Count; i++)
        {
            var existingColumn = inputColumns[i];
            var newColumn = memTable.AddColumnToSchema(
                existingColumn.Name,
                existingColumn.DataType,
                existingColumn.SourceTableName,
                existingColumn.SourceTableAlias
            );
            outputColumns.Add(newColumn);
            outputColumnsRefs.Add(newColumn.ColumnRef);
        }

        var expressions = _binder.Bind(context, top.OrderBy, inputColumns);

        return new TopNSortOperator(
            top.Count,
            bufferPool,
            memTable,
            input,
            expressions,
            outputColumns,
            outputColumnsRefs
            );
    }

    private IOperation CreatePlanWithSubqueries(
        PlanWithSubQueries planWithSub,
        IOperation main,
        List<IOperation> uncorrelatedPlans
        )
    {
        var intermediateOutputs = planWithSub.Uncorrelated.Select(p => p.PreBoundOutputs).ToList();
        foreach (var output in intermediateOutputs.SelectMany(p => p))
        {
            if (output.ColumnRef == default)
            {
                throw new QueryPlanException($"Output column {output.Name} is not bound");
            }
        }

        return new SubqueryOperator(
            bufferPool,
            uncorrelatedPlans,
            intermediateOutputs,
            main
        );
    }

    private IOperation CreateLimit(Limit limit, IOperation input, BindContext context)
    {
        var inputColumns = input.Columns;

        var memRef = catalog.OpenMemoryTable();
        var memTable = bufferPool.GetMemoryTable(memRef.TableId);

        var outputColumns = new List<ColumnSchema>(inputColumns.Count);
        var outputColumnsRefs = new List<ColumnRef>(inputColumns.Count);
        for (var i = 0; i < inputColumns.Count; i++)
        {
            var existingColumn = inputColumns[i];
            var newColumn = memTable.AddColumnToSchema(
                existingColumn.Name,
                existingColumn.DataType,
                existingColumn.SourceTableName,
                existingColumn.SourceTableAlias
            );
            outputColumns.Add(newColumn);
            outputColumnsRefs.Add(newColumn.ColumnRef);
        }

        return new LimitOperator(
            bufferPool,
            memTable,
            input,
            limit.Count,
            outputColumns,
            outputColumnsRefs);
    }

    private IOperation CreateScan(Scan scan, BindContext context)
    {
        var outputColumns = scan.OutputSchema;
        var columnRefs = outputColumns.Select(c => c.ColumnRef).ToList();

        var table = catalog.Tables.SingleOrDefault(t => t.Id == scan.TableId);
        if (table == null)
        {
            var memTable = bufferPool.GetMemoryTable(scan.TableId);
            return new ScanMemoryTable(
                memTable,
                catalog,
                outputColumns,
                columnRefs
            );
        }

        if (TryBindPushFilterDown(context, table, scan.Filter, out var filter))
        {
            return new FileScanFusedFilter(
                bufferPool,
                table.Location,
                catalog,
                filter,
                outputColumns,
                columnRefs
            );
        }

        return new FileScan(
            bufferPool,
            catalog,
            table.Location,
            outputColumns,
            columnRefs
            );
    }

    private bool TryBindPushFilterDown(
        BindContext context,
        TableSchema table,
        BaseExpression? where,
        [NotNullWhen(true)] out BaseExpression? result)
    {
        result = null;
        if (!config.LogicalOptimization)
        {
            // TODO this should be in the logical optimization code?
            return false;
        }

        if (where is not BinaryExpression b)
        {
            return false;
        }

        if (b.Operator != LESS && b.Operator != LESS_EQUAL && b.Operator != GREATER && b.Operator != GREATER_EQUAL)
        {
            return false;
        }

        TokenType op = b.Operator;
        string opLiteral = b.OperatorLiteral;
        ColumnExpression? left = null;
        LiteralExpression? right = null;

        // Normalize to column on left of expression
        if (b.Left is LiteralExpression lit && b.Right is ColumnExpression col)
        {
            left = col;
            right = lit;
            (op, opLiteral) = FlipOperator(op);
        }
        else if (b.Left is ColumnExpression col2 && b.Right is LiteralExpression lit2)
        {
            left = col2;
            right = lit2;
        }
        else
        {
            return false;
        }

        var statTable = bufferPool.GetMemoryTable(table.StatsTable.TableId);

        // What is the correct way to rewrite these?
        // As a range operation with an OR between left and right?
        var leftStat = op switch
        {
            LESS => "_$min",
            LESS_EQUAL => "_$min",
            GREATER => "_$max",
            GREATER_EQUAL => "_$max",
            _ => null,
        };
        if (leftStat == null)
        {
            return false;
        }

        // a < 123 -> min(a) < 123
        // a <= 123 -> min(a) <= 123
        // a > 123 -> max(a) > 123
        // a >= 123 -> max(a) > 123

        result = b with
        {
            Operator = op,
            OperatorLiteral = opLiteral,
            Left = RebindToStatistic(left, leftStat),
            Right = right,
        };

        result = _binder.Bind(context, result, statTable.Schema);

        return true;

        (TokenType, string) FlipOperator(TokenType op)
        {
            return op switch
            {
                LESS => (GREATER, ">"),
                LESS_EQUAL => (GREATER_EQUAL, ">="),
                GREATER => (LESS, "<"),
                GREATER_EQUAL => (LESS_EQUAL, "<="),
                _ => throw new QueryPlanException($"Unexpected operator {op}"),
            };
        }

        BaseExpression RebindToStatistic(BaseExpression expr, string statName)
        {
            if (expr is LiteralExpression)
            {
                return expr;
            }

            if (expr is ColumnExpression c)
            {
                var expectedName = $"{c.Column}{statName}";
                ColumnRef colRef = default;
                var columns = statTable.Schema;
                for (var i = 0; i < columns.Count; i++)
                {
                    if (columns[i].Name == expectedName)
                    {
                        colRef = columns[i].ColumnRef;
                        break;
                    }
                }
                if (colRef == default)
                {
                    throw new QueryPlanException($"Column '{expectedName}' not found in table '{table.Name}'");
                }

                return c with
                {
                    Table = null, // TODO should be bound to the stat table name or something
                    Column = expectedName,
                    BoundOutputColumn = colRef,
                };
            }
            throw new QueryPlanException($"Expression {expr} is not supported for pushdown predicate");
        }
    }

    private IOperation CreateJoin(Join join, IOperation left, IOperation right, BindContext context)
    {
        var inputColumns = QueryPlanner.ExtendSchema(left.Columns, right.Columns);
        var memRef = catalog.OpenMemoryTable();
        var memTable = bufferPool.GetMemoryTable(memRef.TableId);

        var outputColumns = new List<ColumnSchema>(inputColumns.Count);
        var outputColumnsRefs = new List<ColumnRef>(inputColumns.Count);

        var numOutputColumns = join.JoinType == JoinType.Semi
            ? left.Columns.Count
            : inputColumns.Count;

        for (var i = 0; i < numOutputColumns; i++)
        {
            var existingColumn = inputColumns[i];
            var newColumn = memTable.AddColumnToSchema(
                existingColumn.Name,
                existingColumn.DataType,
                existingColumn.SourceTableName,
                existingColumn.SourceTableAlias
            );
            outputColumns.Add(newColumn);
            outputColumnsRefs.Add(newColumn.ColumnRef);
        }

        if (join.JoinType == JoinType.Cross)
        {
            var expression = join.Condition != null ? _binder.Bind(context, join.Condition!, inputColumns) : null;

            return new NestedLoopJoinOperator(
                bufferPool,
                left,
                right,
                memTable,
                expression,
                outputColumns,
                outputColumnsRefs
            );
        }

        if (join.JoinType == JoinType.Inner || join.JoinType == JoinType.Semi)
        {
            var expressions = _binder.Bind(context, join.Condition!, inputColumns);
            // TODO split join condition
            if (expressions is not BinaryExpression b || b.Operator != EQUAL)
            {
                throw new QueryPlanException($"Join condition must be a binary expression with EQUAL operator");
            }
            if (b.Left is not ColumnExpression leftExpr || b.Right is not ColumnExpression rightExpr)
            {
                throw new QueryPlanException($"Join condition must be a simple binary expression with EQUAL operator" +
                                             $"on column expressions");
            }

            // TODO the left/right on the plan have aliases attached (Scan op, might need to expose more generically)
            // maybe I can use them to figureout which side to bind
            BaseExpression scanExpr;
            BaseExpression probeExpr;
            try
            {
                scanExpr = _binder.Bind(context, leftExpr, left.Columns);
                probeExpr = _binder.Bind(context, rightExpr, right.Columns);
            }
            catch (QueryPlanException e) when (e.Message.Contains("was not found in the list of available columns"))
            {
                scanExpr = _binder.Bind(context, rightExpr, left.Columns);
                probeExpr = _binder.Bind(context, leftExpr, right.Columns);
            }

            return new HashJoinOperator(
                join.JoinType,
                bufferPool,
                left,
                right,
                memTable,
                [scanExpr],
                [probeExpr],
                outputColumns,
                outputColumnsRefs
            );
        }

        throw new NotImplementedException($"Currently only support inner join and cross join, got {join.JoinType} join");
    }

    private IOperation CreateAggregate(Aggregate aggregate, IOperation source, BindContext context)
    {
        var inputColumns = source.Columns;
        var expressions = _binder.Bind(context, aggregate.Aggregates, inputColumns);
        var groupingExprs = _binder.Bind(context, aggregate.GroupBy, inputColumns);


        // Transformation 1. Grouping + Aggregation
        var memRef1 = catalog.OpenMemoryTable();
        var memTable = bufferPool.GetMemoryTable(memRef1.TableId);

        var outputExpressions = new List<BaseExpression>(expressions.Count);
        var outputColumns = new List<ColumnSchema>(expressions.Count);
        for (var i = 0; i < groupingExprs.Count; i++)
        {
            var expr = groupingExprs[i];
            string? sourceTable = null;
            if (expr is ColumnExpression colExpr)
            {
                sourceTable = colExpr.Table;
            }

            var newColumn = memTable.AddColumnToSchema(
                expr.Alias,
                expr.BoundFunction!.ReturnType,
                aggregate.Alias ?? sourceTable ?? "",
                aggregate.Alias ?? sourceTable ?? "");

            outputExpressions.Add(expr with
            {
                BoundOutputColumn = newColumn.ColumnRef,
            });
            outputColumns.Add(newColumn);
        }

        var aggregateExpressions = expressions
            .Where(e => e.BoundFunction is IAggregateFunction)
            .ToList();

        foreach (var expr in aggregateExpressions)
        {
            var newColumn = memTable.AddColumnToSchema(
                expr.Alias,
                expr.BoundFunction!.ReturnType,
                aggregate.Alias ?? "",
                aggregate.Alias ?? ""
                );
            outputExpressions.Add(expr with
            {
                BoundOutputColumn = newColumn.ColumnRef,
            });
            outputColumns.Add(newColumn);
        }

        var outputColumnRefs = outputColumns.Select(c => c.ColumnRef).ToList();

        if (aggregate.GroupBy.Count == 0)
        {
            return new UngroupedAggregate(
                bufferPool,
                memTable,
                source,
                outputExpressions,
                outputColumns,
                outputColumnRefs);
        }

        return new HashAggregate(
            bufferPool,
            source,
            memTable,
            outputExpressions,
            outputColumns,
            outputColumnRefs);
    }

    private IOperation CreateSort(
        Sort sort,
        IOperation input,
        BindContext context)
    {
        var memRef = catalog.OpenMemoryTable();
        var memTable = bufferPool.GetMemoryTable(memRef.TableId);

        var inputColumns = input.Columns;
        var outputColumns = new List<ColumnSchema>(inputColumns.Count);
        var outputColumnsRefs = new List<ColumnRef>(inputColumns.Count);
        for (var i = 0; i < inputColumns.Count; i++)
        {
            var existingColumn = inputColumns[i];
            var newColumn = memTable.AddColumnToSchema(
                existingColumn.Name,
                existingColumn.DataType,
                existingColumn.SourceTableName,
                existingColumn.SourceTableAlias
                );
            outputColumns.Add(newColumn);
            outputColumnsRefs.Add(newColumn.ColumnRef);
        }

        var orderExpressions = _binder.Bind(context, sort.OrderBy, inputColumns);
        var sortColumns = new List<ColumnSchema>(sort.OrderBy.Count);
        for (var i = 0; i < sort.OrderBy.Count; i++)
        {
            var expr = sort.OrderBy[i];
            var newColumn = memTable.AddColumnToSchema(
                expr.Alias,
                expr.BoundFunction!.ReturnType,
                "",
                ""
                );
            sortColumns.Add(newColumn);
        }

        return new SortOperator(
            bufferPool,
            memTable,
            input,
            orderExpressions,
            sortColumns,
            outputColumns,
            outputColumnsRefs);
    }

    private IOperation CreateDistinct(Distinct distinct, IOperation source, BindContext context)
    {
        var inputColumns = source.Columns;
        var memRef = catalog.OpenMemoryTable();
        var memTable = bufferPool.GetMemoryTable(memRef.TableId);

        var outputColumns = new List<ColumnSchema>(inputColumns.Count);
        var outputColumnsRefs = new List<ColumnRef>(inputColumns.Count);
        for (var i = 0; i < inputColumns.Count; i++)
        {
            var existingColumn = inputColumns[i];
            var newColumn = memTable.AddColumnToSchema(
                existingColumn.Name,
                existingColumn.DataType,
                existingColumn.SourceTableName,
                existingColumn.SourceTableAlias
                );
            outputColumns.Add(newColumn);
            outputColumnsRefs.Add(newColumn.ColumnRef);
        }

        return new DistinctOperation(
            bufferPool,
            memTable,
            source,
            outputColumns,
            outputColumnsRefs);
    }

    private IOperation CreateProjection(Projection projection, IOperation input, BindContext context)
    {
        var expressions = _binder.Bind(context, projection.Expressions, input.Columns);
        var usedColumns = expressions.Select(e => e.Alias).ToHashSet();

        var mutExprssions = expressions.ToList();

        // TODO I still kinda dislike this
        if (projection.AppendExpressions)
        {
            foreach (var col in input.Columns)
            {
                if (!usedColumns.Contains(col.Name))
                {
                    var sourceTable = col.SourceTableAlias != ""
                        ? col.SourceTableAlias
                        : col.SourceTableName;
                    mutExprssions.Add(new ColumnExpression(col.Name, sourceTable));
                }
            }
            expressions = _binder.Bind(context, mutExprssions, input.Columns);
        }

        var memRef = catalog.OpenMemoryTable();
        var memTable = bufferPool.GetMemoryTable(memRef.TableId);

        var outputExpressions = new List<BaseExpression>(expressions.Count);
        var outputColumns = new List<ColumnSchema>(expressions.Count);

        for (var i = 0; i < expressions.Count; i++)
        {
            var expr = expressions[i];
            var tableName = projection.Alias;

            // This is a pushdown projection, keep the original
            // table name and aliases intact
            if (projection.AppendExpressions)
            {
                if (expr is ColumnExpression e)
                {
                    tableName = e.Table;
                }
            }

            var newColumn = memTable.AddColumnToSchema(
                expr.Alias,
                expr.BoundFunction!.ReturnType,
                tableName ?? "",
                tableName ?? ""
                );

            outputExpressions.Add(expr with
            {
                BoundOutputColumn = newColumn.ColumnRef,
            });
            outputColumns.Add(newColumn);
        }

        var outputColumnRefs = outputColumns.Select(c => c.ColumnRef).ToList();
        return new ProjectionOperation(
            bufferPool,
            memTable,
            input,
            outputExpressions,
            outputColumns,
            outputColumnRefs,
            Materialize: false
            );
    }

    private IOperation CreateFilter(Filter filter, IOperation input, BindContext context)
    {
        var inputColumns = input.Columns;
        var whereExpr = _binder.Bind(context, filter.Predicate, inputColumns);
        if (whereExpr.BoundFunction!.ReturnType != DataType.Bool)
        {
            // TODO cast values to "truthy"
            throw new QueryPlanException($"Filter expression '{filter.Predicate}' is not a boolean expression");
        }

        whereExpr = whereExpr.Rewrite(f =>
        {
            if (f is SubQueryResultExpression { Correlated: true, } subQuery
                && f.BoundFunction is CorrelatedSubQueryFunction { SourceInputColumns: [] } subFn)
            {
                var inputTableRef = subQuery.BoundInputMemoryTable;
                var inputTable = bufferPool.GetMemoryTable(inputTableRef.TableId);
                var subQueryInputColumns = inputTable.Schema;

                var sourceInputColumns = new List<ColumnSchema>(subQueryInputColumns.Count);
                foreach (var col in subQueryInputColumns)
                {
                    // TODO this is essentially a bind, can we unify it?
                    // Maybe make the subquery columns available in the Bind context?
                    var matching = inputColumns.Single(c => c.Name == col.Name);
                    sourceInputColumns.Add(matching);
                }

                if (sourceInputColumns.Count == 0)
                {
                    return f;
                }

                return subQuery with
                {
                    BoundFunction = subFn with
                    {
                        SourceInputColumns = sourceInputColumns,
                        SubQueryCopyInputColumns = subQueryInputColumns,
                    },
                };
            }
            return f;
        });

        var memRef = catalog.OpenMemoryTable();
        var memTable = bufferPool.GetMemoryTable(memRef.TableId);

        var outputColumns = new List<ColumnSchema>(inputColumns.Count);
        var outputColumnsRefs = new List<ColumnRef>(inputColumns.Count);
        for (var i = 0; i < inputColumns.Count; i++)
        {
            var existingColumn = inputColumns[i];
            var newColumn = memTable.AddColumnToSchema(
                existingColumn.Name,
                existingColumn.DataType,
                existingColumn.SourceTableName,
                existingColumn.SourceTableAlias
                );
            outputColumns.Add(newColumn);
            outputColumnsRefs.Add(newColumn.ColumnRef);
        }

        return new FilterOperation(
            bufferPool,
            memTable,
            input,
            whereExpr,
            outputColumns,
            outputColumnsRefs);
    }
}
