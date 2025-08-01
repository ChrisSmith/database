using System.Diagnostics.CodeAnalysis;
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
    private ExpressionBinder _binder = new(bufferPool, new FunctionRegistry());

    public LogicalPlan CreateLogicalPlan(IStatement statement)
    {
        if (statement is not SelectStatement select)
        {
            throw new QueryPlanException(
                $"Unknown statement type '{statement.GetType().Name}'. Cannot create query plan.");
        }

        select = ConstantFolding.Fold(select);
        select = QueryRewriter.ExpandStarStatements(select, catalog);

        if (select.From.TableStatements.Single() is not TableStatement singleTable)
        {
            throw new QueryPlanException("Expected a single table in FROM clause.");
        }

        var expressions = select.SelectList.Expressions;
        var plan = BindLogicalScan(singleTable, select);

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
                expr.BoundFunction!.ReturnType.ClrTypeFromDataType()
                ));
        }

        return schema;
    }

    private LogicalPlan BindLogicalScan(TableStatement singleTable, SelectStatement select)
    {
        var table = catalog.Tables.FirstOrDefault(t => t.Name == singleTable.Table);
        if (table == null)
        {
            throw new QueryPlanException($"Table '{singleTable.Table}' not found in catalog.");
        }

        IReadOnlyList<ColumnSchema> tableColumns = table.Columns.Select(c => c).ToList();

        // Projection Push Down
        tableColumns = FilterToUsedColumns(select, tableColumns);

        LogicalPlan plan = new Scan(
            singleTable.Table,
            table.Id,
            select.Where,
            tableColumns,
            singleTable.Alias);
        return plan;
    }

    public IOperation CreatePhysicalPlan(LogicalPlan plan)
    {
        if (plan is Scan scan)
        {
            return CreateScan(scan);
        }
        if (plan is Filter filter)
        {
            var input = CreatePhysicalPlan(filter.Input);
            return CreateFilter(filter, input);
        }

        if (plan is Join join)
        {
            return CreateJoin(join);
        }

        if (plan is Aggregate aggregate)
        {
            var input = CreatePhysicalPlan(aggregate.Input);
            return CreateAggregate(aggregate, input);
        }

        if (plan is Projection project)
        {
            var input = CreatePhysicalPlan(project.Input);
            return CreateProjection(project, input);
        }

        if (plan is Distinct distinct)
        {
            var input = CreatePhysicalPlan(distinct.Input);
            return CreateDistinct(distinct, input);
        }

        if (plan is Sort sort)
        {
            var input = CreatePhysicalPlan(sort.Input);
            return CreateSort(sort, input);
        }

        throw new NotImplementedException();
    }

    public QueryPlan CreatePlan(IStatement statement)
    {
        var logicalPlan = CreateLogicalPlan(statement);
        var physicalPlan = CreatePhysicalPlan(logicalPlan);
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
                expr.BoundFunction!.ReturnType.ClrTypeFromDataType()
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

    private IOperation CreateScan(Scan scan)
    {
        var outputColumns = scan.OutputColumns;

        var table = catalog.Tables.Single(t => t.Id == scan.TableId);
        var columnRefs = outputColumns.Select(c => c.ColumnRef).ToList();

        if (TryBindPushFilterDown(table, scan.Filter, out var filter))
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
            table.Location,
            outputColumns,
            columnRefs
            );
    }

    private bool TryBindPushFilterDown(
        TableSchema table,
        BaseExpression? where,
        [NotNullWhen(true)] out BaseExpression? result)
    {
        result = null;
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

        result = _binder.Bind(result, statTable.Schema);

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
                    Column = expectedName,
                    BoundOutputColumn = colRef,
                };
            }
            throw new QueryPlanException($"Expression {expr} is not supported for pushdown predicate");
        }
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

    private IOperation CreateJoin(Join join)
    {
        throw new NotImplementedException();
    }

    private IOperation CreateAggregate(Aggregate aggregate, IOperation source)
    {
        var inputColumns = source.Columns;
        var expressions = _binder.Bind(aggregate.Aggregates, inputColumns);
        var groupingExprs = _binder.Bind(aggregate.GroupBy, inputColumns);


        // Transformation 1. Grouping + Aggregation
        var memRef1 = bufferPool.OpenMemoryTable();
        var memTable = bufferPool.GetMemoryTable(memRef1.TableId);

        var outputExpressions = new List<BaseExpression>(expressions.Count);
        var outputColumns = new List<ColumnSchema>(expressions.Count);
        for (var i = 0; i < groupingExprs.Count; i++)
        {
            var expr = groupingExprs[i];
            var newColumn = memTable.AddColumnToSchema(expr.Alias, expr.BoundFunction!.ReturnType);

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
            var newColumn = memTable.AddColumnToSchema(expr.Alias, expr.BoundFunction!.ReturnType);
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
        IOperation input)
    {
        var memRef = bufferPool.OpenMemoryTable();
        var memTable = bufferPool.GetMemoryTable(memRef.TableId);

        var inputColumns = input.Columns;
        var outputColumns = new List<ColumnSchema>(inputColumns.Count);
        var outputColumnsRefs = new List<ColumnRef>(inputColumns.Count);
        for (var i = 0; i < inputColumns.Count; i++)
        {
            var existingColumn = inputColumns[i];
            var newColumn = memTable.AddColumnToSchema(existingColumn.Name, existingColumn.DataType);
            outputColumns.Add(newColumn);
            outputColumnsRefs.Add(newColumn.ColumnRef);
        }

        var orderExpressions = _binder.Bind(sort.OrderBy, inputColumns);
        var sortColumns = new List<ColumnSchema>(sort.OrderBy.Count);
        for (var i = 0; i < sort.OrderBy.Count; i++)
        {
            var expr = sort.OrderBy[i];
            var newColumn = memTable.AddColumnToSchema(expr.Alias, expr.BoundFunction!.ReturnType);
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

    private IOperation CreateDistinct(Distinct distinct, IOperation source)
    {
        var inputColumns = source.Columns;
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

        return new DistinctOperation(
            bufferPool,
            memTable,
            source,
            outputColumns,
            outputColumnsRefs);
    }

    private IOperation CreateProjection(Projection projection, IOperation input)
    {
        var expressions = _binder.Bind(projection.Expressions, input.Columns);
        var usedColumns = expressions.Select(e => e.Alias).ToHashSet();

        var mutExprssions = expressions.ToList();

        // TODO I still kinda dislike this
        if (projection.AppendExpressions)
        {
            foreach (var col in input.Columns)
            {
                if (!usedColumns.Contains(col.Name))
                {
                    mutExprssions.Add(new ColumnExpression(col.Name));
                }
            }
            expressions = _binder.Bind(mutExprssions, input.Columns);
        }

        var memRef = bufferPool.OpenMemoryTable();
        var memTable = bufferPool.GetMemoryTable(memRef.TableId);

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

    private IOperation CreateFilter(Filter filter, IOperation input)
    {
        var inputColumns = input.Columns;
        var whereExpr = _binder.Bind(filter.Predicate, inputColumns);
        if (whereExpr.BoundFunction is not BoolFunction predicate)
        {
            // TODO cast values to "truthy"
            throw new QueryPlanException($"Filter expression '{filter.Predicate}' is not a boolean expression");
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

        return new FilterOperation(
            bufferPool,
            memTable,
            input,
            whereExpr,
            outputColumns,
            outputColumnsRefs);
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
}

public class QueryPlanException(string message) : Exception(message);
