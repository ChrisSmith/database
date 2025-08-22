using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;
using Database.Core.Options;
using Database.Core.Planner.QueryGraph;

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
        _optimizer = new QueryOptimizer(_options, _binder, bufferPool);
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
        select = QueryRewriter.DuplicateSelectExpressions(select);

        var allQueryPlans = new List<LogicalPlan>();
        var allSubPlanContext = new List<BindContext>();

        if (select.Where != null)
        {
            var (updatedWhere, subQueryStatements) = QueryRewriter.ExtractSubqueries(select.Where, subQueryId: allQueryPlans.Count);

            (var queryPlans, var subPlanContext, updatedWhere) = ProcessSubQueries(context, updatedWhere, subQueryStatements);
            allQueryPlans.AddRange(queryPlans);
            allSubPlanContext.AddRange(subPlanContext);

            select = select with { Where = updatedWhere };
        }

        if (select.Having != null)
        {
            var (updatedHaving, subQueryStatements) = QueryRewriter.ExtractSubqueries(select.Having, subQueryId: allQueryPlans.Count);

            (var queryPlans, var subPlanContext, updatedHaving) = ProcessSubQueries(context, updatedHaving, subQueryStatements);
            allQueryPlans.AddRange(queryPlans);
            allSubPlanContext.AddRange(subPlanContext);

            select = select with { Having = updatedHaving };
        }

        var expressions = select.SelectList.Expressions;
        var plan = BindRelations(context, select);

        // Must bind here before ExpressionContainsAggregate so there are functions to check
        expressions = _binder.Bind(context, expressions, plan.OutputSchema);
        if (select.Group?.Expressions != null || expressions.Any(ExpressionContainsAggregate))
        {
            var groupingExprs = _binder.Bind(context, select.Group?.Expressions ?? [], plan.OutputSchema);
            (var aggregates, expressions) = SeparateAggregatesFromExpressions(expressions);

            // if the groupings are not in the expressions, add them
            foreach (var groupingExpr in groupingExprs)
            {
                if (!aggregates.Contains(groupingExpr))
                {
                    aggregates.Add(groupingExpr);
                }
            }

            // Might throw this one away if we still need to extend the schema
            var aggregatePlan = new Aggregate(plan, groupingExprs, aggregates, select.Alias);

            if (select.Having != null)
            {
                var havingExpr = _binder.Bind(context, select.Having, plan.OutputSchema, ignoreMissingColumns: true);
                var (havingAggregates, havingExprs) = SeparateAggregatesFromExpressions([havingExpr]);

                var modified = false;
                foreach (var havingAggregate in havingAggregates)
                {
                    if (!aggregates.Contains(havingAggregate))
                    {
                        aggregates.Add(havingAggregate);
                        modified = true;
                    }
                }

                if (modified)
                {
                    aggregatePlan = new Aggregate(plan, groupingExprs, aggregates, select.Alias);
                }

                plan = new Filter(aggregatePlan, havingExprs.Single());
            }
            else
            {
                plan = aggregatePlan;
            }

            expressions = _binder.Bind(context, expressions, plan.OutputSchema);
        }
        else if (select.Having != null)
        {
            throw new QueryPlanException("HAVING clause can only be used with GROUP BY or Aggregates in the select clause");
        }

        if (select.Order != null)
        {
            var orderBy = _binder.Bind(context, select.Order.Expressions, plan.OutputSchema);
            plan = new Sort(plan, orderBy);
            expressions = _binder.Bind(context, expressions, plan.OutputSchema);
        }

        expressions = _binder.Bind(context, expressions, plan.OutputSchema);
        plan = new Projection(
            plan,
            expressions,
            SchemaFromExpressions(expressions, select.Alias),
            Alias: select.Alias);

        if (select.SelectList.Distinct)
        {
            plan = new Distinct(plan);
        }

        if (select.Limit != null)
        {
            plan = new Limit(plan, select.Limit.Count);
        }

        if (allQueryPlans.Count > 0)
        {
            return new PlanWithSubQueries(plan, allQueryPlans, allSubPlanContext);
        }
        return plan;
    }

    private (List<LogicalPlan>, List<BindContext>, BaseExpression) ProcessSubQueries(
        BindContext context,
        BaseExpression expression,
        List<SubQueryPlan> subQueryStatements
        )
    {
        // IFF the subqueries are uncorrelated we can bind them first. they'll be run first
        var queryPlans = new List<LogicalPlan>();
        var subPlanContext = new List<BindContext>();
        var updatedExpression = expression;

        for (var i = 0; i < subQueryStatements.Count; i++)
        {
            var subQueryStmt = subQueryStatements[i];
            var bindContext = new BindContext();

            var memRef = _bufferPool.OpenMemoryTable();
            var table = _bufferPool.GetMemoryTable(memRef.TableId);

            if (subQueryStmt is SubQuerySelectPlan subQueryPlan)
            {
                var subPlan = CreateLogicalPlan(subQueryPlan.Select, bindContext);
                if (subPlan.OutputSchema.Count != 1)
                {
                    throw new QueryPlanException("Subquery must return a single column.");
                }

                var sourceCol = subPlan.OutputSchema[0];
                var newColumn = table.AddColumnToSchema(sourceCol.Name, sourceCol.DataType, "", "");

                subPlan = subPlan with
                {
                    PreBoundOutputs = [newColumn],
                };

                var subQueryId = subQueryPlan.Expression.SubQueryId;
                subQueryStatements[i] = subQueryPlan = subQueryPlan with
                {
                    Expression = subQueryPlan.Expression with
                    {
                        BoundDataType = sourceCol.DataType,
                        BoundOutputColumn = newColumn.ColumnRef,
                        BoundMemoryTable = memRef,
                    },
                };

                context.AddSymbol(subQueryPlan.Expression);

                // The subquery output now has an output table/type.
                // Bind it to the things reading from it
                updatedExpression = updatedExpression.Rewrite(e =>
                {
                    if (e is SubQueryResultExpression re && re.SubQueryId == subQueryId)
                    {
                        return subQueryPlan.Expression;
                    }
                    return e;
                });

                queryPlans.Add(subPlan);
                subPlanContext.Add(bindContext);
            }
            else if (subQueryStmt is SubQueryInPlan inPlan)
            {
                var bound = (ExpressionList)_binder.Bind(context, inPlan.ExpressionList, []);
                var dataType = bound.Statements[0].BoundDataType!.Value;
                var columnName = inPlan.Expression.Alias;
                var newColumn = table.AddColumnToSchema(columnName, dataType, "", "");

                var rg = table.AddRowGroup();
                var array = Array.CreateInstance(dataType.ClrTypeFromDataType(), bound.Statements.Count);
                // Insert the rows into the table
                for (var j = 0; j < bound.Statements.Count; j++)
                {
                    var litExpr = (LiteralExpression)bound.Statements[j];
                    object val = litExpr switch
                    {
                        BoolLiteral boolLiteral => boolLiteral.Literal,
                        DateLiteral dateLiteral => dateLiteral.Literal,
                        DateTimeLiteral dateTimeLiteral => dateTimeLiteral.Literal,
                        DecimalLiteral decimalLiteral => decimalLiteral.Literal,
                        IntegerLiteral integerLiteral => integerLiteral.Literal,
                        IntervalLiteral intervalLiteral => intervalLiteral.Literal,
                        StringLiteral stringLiteral => stringLiteral.Literal,
                        NullLiteral nullLiteral => throw new NotImplementedException(),
                        _ => throw new ArgumentOutOfRangeException()
                    };
                    array.SetValue(val, j);
                }

                var column = ColumnHelper.CreateColumn(dataType.ClrTypeFromDataType(), columnName, array);
                table.PutColumn(newColumn.ColumnRef with { RowGroup = rg.RowGroup }, column);

                var subQueryId = inPlan.Expression.SubQueryId;
                subQueryStatements[i] = inPlan = inPlan with
                {
                    Expression = inPlan.Expression with
                    {
                        BoundDataType = newColumn.DataType,
                        BoundOutputColumn = newColumn.ColumnRef,
                        BoundMemoryTable = memRef,
                    },
                };

                context.AddSymbol(inPlan.Expression);

                // The subquery output now has an output table/type.
                // Bind it to the things reading from it
                updatedExpression = updatedExpression.Rewrite(e =>
                {
                    if (e is SubQueryResultExpression re && re.SubQueryId == subQueryId)
                    {
                        return inPlan.Expression;
                    }
                    return e;
                });
            }
            else
            {
                throw new QueryPlanException($"Unknown subquery type. {subQueryStmt}");
            }
        }

        return (queryPlans, subPlanContext, updatedExpression);
    }

    public static IReadOnlyList<ColumnSchema> SchemaFromExpressions(
        IReadOnlyList<BaseExpression> expressions,
        string? selectAlias)
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
                SourceTableName: selectAlias ?? "",
                SourceTableAlias: selectAlias ?? ""
                ));
        }

        return schema;
    }

    private LogicalPlan BindRelations(BindContext context, SelectStatement select)
    {
        List<BaseExpression> conjunctions = SplitConjunctions(select.Where);

        var relations = new List<JoinedRelation>();
        foreach (var table in select.From.TableStatements)
        {
            if (table is TableStatement tableStmt)
            {
                relations.Add(new JoinedRelation(
                    tableStmt.Alias ?? tableStmt.Table,
                    CreateScanForTable(tableStmt),
                    JoinType.Cross
                    ));
            }
            else if (table is SelectStatement selectStmt)
            {
                // TODO I might need to move binding till after all base columns from all scans
                // including nested queries have been placed into context

                var name = selectStmt.Alias ?? throw new QueryPlanException($"expression '{selectStmt}' has no alias.");
                relations.Add(new JoinedRelation(
                    name,
                    CreateLogicalPlan(selectStmt, context),
                    JoinType.Cross
                ));
            }
            else
            {
                throw new QueryPlanException($"Unknown table statement type '{table.GetType().Name}'. Cannot create query plan.");
            }
        }

        // let's see if they are helping us with the join type
        if (select.From.JoinStatements != null)
        {
            foreach (var join in select.From.JoinStatements)
            {
                var tableStmt = (TableStatement)join.Table;
                var scan = CreateScanForTable(tableStmt);
                relations.Add(new JoinedRelation(
                    tableStmt.Alias ?? tableStmt.Table,
                    scan,
                    join.JoinType
                    ));

                // TODO I don't think this is right
                // It would be better to split here and directly create edges based
                // on the tables seen in the predicates?
                conjunctions.AddRange(SplitConjunctions(join.JoinConstraint));
            }
        }

        var edges = new List<Edge>();
        var filters = new List<BaseExpression>();

        foreach (var expr in conjunctions)
        {
            var found = false;
            for (var i = 0; i < relations.Count; i++)
            {
                var one = relations[i];
                // TODO do this without exceptions
                try
                {
                    _ = _binder.Bind(context, expr, one.Plan.OutputSchema);
                    edges.Add(new UnaryEdge(one.Name, expr));
                    found = true;
                    break;
                }
                catch (QueryPlanException)
                {
                }
            }

            for (var i = 0; i < relations.Count && !found; i++)
            {
                var one = relations[i];
                for (var j = 0; j < relations.Count && !found; j++)
                {
                    if (i == j) { continue; }
                    var two = relations[j];

                    try
                    {
                        var mergedSchema = ExtendSchema(one.Plan.OutputSchema, two.Plan.OutputSchema);
                        _ = _binder.Bind(context, expr, mergedSchema);
                        edges.Add(new BinaryEdge(one.Name, two.Name, expr));
                        found = true;
                        break;
                    }
                    catch (QueryPlanException)
                    {
                    }
                }
            }

            if (!found)
            {
                filters.Add(expr);
            }
        }

        // Convert any cross joins to inner joins if we have an equi-join
        var equiJoins = edges.Where(edge => edge is BinaryEdge
        {
            Expression: BinaryExpression { Operator: TokenType.EQUAL }
        })
        .Cast<BinaryEdge>()
        .ToList();

        for (var i = 0; i < relations.Count; i++)
        {
            foreach (var edge in equiJoins)
            {
                var name = relations[i].Name;
                if (name == edge.One || name == edge.Two)
                {
                    relations[i] = relations[i] with { JoinType = JoinType.Inner };
                    break;
                }
            }
        }

        if (relations.Count == 1)
        {
            if (select.Where != null)
            {
                return new Filter(relations[0].Plan, select.Where);
            }
            return relations[0].Plan;
        }

        return new JoinSet(relations, edges, filters);

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

    private List<BaseExpression> SplitConjunctions(BaseExpression? expr)
    {
        if (expr == null)
        {
            return [];
        }

        var result = new List<BaseExpression>();
        var queue = new Queue<BaseExpression>();
        queue.Enqueue(expr);
        while (queue.Count > 0)
        {
            var current = queue.Dequeue();
            if (current is BinaryExpression { Operator: TokenType.AND } binExpr)
            {
                queue.Enqueue(binExpr.Left);
                queue.Enqueue(binExpr.Right);
            }
            else
            {
                result.Add(current);
            }
        }
        return result;
    }

    public QueryPlan CreatePlan(IStatement statement)
    {
        var context = new BindContext();
        var logicalPlan = CreateLogicalPlan(statement, context);
        logicalPlan = _optimizer.OptimizePlan(logicalPlan, context);
        var physicalPlan = _costBasedOptimizer.OptimizeAndLower(logicalPlan, context);
        return new QueryPlan(physicalPlan);
    }

    private (List<BaseExpression>, List<BaseExpression>) SeparateAggregatesFromExpressions(IReadOnlyList<BaseExpression> expressions)
    {
        // If an expression contains both an aggregate and binary expression, we must run the binary
        // expressions after computing the aggregates
        // So the table fed into the projection will be the result from the aggregation
        // rewrite the expressions to reference the resulting column instead of the agg function
        // Ie. count(Id) + 4 -> col[count] + 4

        var resultAggregates = new List<BaseExpression>();
        var resultExpressions = new List<BaseExpression>(expressions.Count);

        BaseExpression ReplaceAggregate(BaseExpression expr)
        {
            if (expr.BoundFunction is IAggregateFunction)
            {
                resultAggregates.Add(expr);
                return new ColumnExpression(expr.Alias)
                {
                    Alias = expr.Alias,
                    BoundFunction = new SelectFunction(expr.BoundOutputColumn, expr.BoundDataType!.Value, _bufferPool),
                    BoundDataType = expr.BoundDataType!.Value,
                };
            }
            return expr;
        }

        for (var i = 0; i < expressions.Count; i++)
        {
            var rewritten = expressions[i].Rewrite(ReplaceAggregate);
            resultExpressions.Add(rewritten);
        }

        return (resultAggregates, resultExpressions);
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

    public static IReadOnlyList<ColumnSchema> GetCombinedOutputSchema(IEnumerable<LogicalPlan> relations)
    {
        var result = new List<ColumnSchema>();
        foreach (var relation in relations)
        {
            result.AddRange(relation.OutputSchema);
        }
        return result;
    }
}

public class QueryPlanException(string message) : Exception(message);
