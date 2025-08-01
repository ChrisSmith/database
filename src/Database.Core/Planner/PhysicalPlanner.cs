using System.Diagnostics.CodeAnalysis;
using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;
using Database.Core.Operations;
using static Database.Core.TokenType;

namespace Database.Core.Planner;

public class PhysicalPlanner(Catalog.Catalog catalog, ParquetPool bufferPool)
{
    private ExpressionBinder _binder = new(bufferPool, new FunctionRegistry());

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
            var newColumn = memTable.AddColumnToSchema(
                expr.Alias,
                expr.BoundFunction!.ReturnType,
                "",
                "");

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
                "",
                ""
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
            var newColumn = memTable.AddColumnToSchema(
                existingColumn.Name,
                existingColumn.DataType,
                existingColumn.SourceTableName,
                existingColumn.SourceTableAlias
                );
            outputColumns.Add(newColumn);
            outputColumnsRefs.Add(newColumn.ColumnRef);
        }

        var orderExpressions = _binder.Bind(sort.OrderBy, inputColumns);
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
            var newColumn = memTable.AddColumnToSchema(
                existingColumn.Name,
                existingColumn.DataType,
                "",
                ""
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
            var newColumn = memTable.AddColumnToSchema(
                expr.Alias,
                expr.BoundFunction!.ReturnType,
                "",
                ""
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
