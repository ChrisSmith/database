using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;
using Database.Core.Options;
using Database.Core.Planner.QueryGraph;

namespace Database.Core.Planner;

public partial class QueryPlanner
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
        var costEstimation = new CostEstimation(catalog, bufferPool);
        _optimizer = new QueryOptimizer(_options, _binder, bufferPool, costEstimation);
        _physicalPlanner = new PhysicalPlanner(_options, catalog, bufferPool, costEstimation);
        _costBasedOptimizer = new CostBasedOptimizer(_options, _physicalPlanner);
    }

    public LogicalPlan CreateLogicalPlan(IStatement statement, BindContext context)
    {
        if (statement is not SelectStatement select)
        {
            throw new QueryPlanException(
                $"Unknown statement type '{statement.GetType().Name}'. Cannot create query plan.");
        }

        select = ConstantFolding.Simplify(select);
        select = QueryRewriter.ExpandStarStatements(select, _catalog);
        select = QueryRewriter.DuplicateSelectExpressions(select);

        var uncorrelatedPlans = new List<LogicalPlan>();
        var correlatedPlans = new List<LogicalPlan>();

        if (select.Where != null)
        {
            var (updatedWhere, subQueryStatements) = QueryRewriter.ExtractSubqueries(select.Where, subQueryId: uncorrelatedPlans.Count);

            (var queryPlans, updatedWhere) = ProcessSubQueries(context, updatedWhere, subQueryStatements);
            for (var i = 0; i < queryPlans.Count; i++)
            {
                var queryPlan = queryPlans[i];
                if (queryPlan.BindContext!.LateBoundSymbols.Count > 0)
                {
                    correlatedPlans.Add(queryPlan);
                }
                else
                {
                    uncorrelatedPlans.Add(queryPlan);
                }
            }

            select = select with { Where = updatedWhere };
        }

        if (select.Having != null)
        {
            var (updatedHaving, subQueryStatements) = QueryRewriter.ExtractSubqueries(select.Having, subQueryId: uncorrelatedPlans.Count);

            (var queryPlans, updatedHaving) = ProcessSubQueries(context, updatedHaving, subQueryStatements);
            uncorrelatedPlans.AddRange(queryPlans);

            select = select with { Having = updatedHaving };
        }

        var expressions = select.SelectList.Expressions;
        var plan = BindRelations(context, select);

        // Must bind here so that
        // 1. Aggregate expression can be found using ExpressionContainsAggregate
        // 2. Nested Subqueries can be found using ExpressionContainsNestedSubQuery
        expressions = _binder.Bind(context, expressions, plan.OutputSchema);

        if (correlatedPlans.Count > 0)
        {
            // At this point all the table symbols are available, identify and rewrite the nested subqueries to use the intermediate result table
            correlatedPlans = LateBindSymbolsAndAllocateInputTable(context, correlatedPlans);
            // TODO move this around to support subqueries in the having clause
            plan = new Apply(plan, correlatedPlans);
        }

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

        if (uncorrelatedPlans.Count > 0)
        {
            return new PlanWithSubQueries(plan, uncorrelatedPlans);
        }
        return plan with
        {
            BindContext = context,
        };
    }

    private List<LogicalPlan> LateBindSymbolsAndAllocateInputTable(
        BindContext parentContext,
        List<LogicalPlan> correlatedPlans
        )
    {
        var rewrittenPlans = new List<LogicalPlan>(correlatedPlans.Count);
        for (var i = 0; i < correlatedPlans.Count; i++)
        {
            var plan = correlatedPlans[i];
            var subQueryContext = plan.BindContext!;
            var (subQueryId, tableRef) = parentContext.CorrelatedSubQueryInputs.Single(); // TODO
            var table = _bufferPool.GetMemoryTable(tableRef.TableId);

            foreach (var (columnName, tableAlias) in subQueryContext.LateBoundSymbols.Keys)
            {
                if (!parentContext.ReferenceSymbol(columnName, tableAlias, out var symbol))
                {
                    throw new QueryPlanException($"Failed to resolve symbol '{tableAlias}.{columnName}'");
                }

                var columnSchema = table.AddColumnToSchema(
                    symbol.Name,
                    symbol.DataType,
                    symbol.TableName,
                    tableAlias!
                );

                var ident = tableAlias == null
                    ? columnName
                    : $"{tableAlias}.{columnName}";

                var copiedSymbol = new BindSymbol(columnName, symbol.TableName, symbol.DataType, columnSchema.ColumnRef, 1);
                subQueryContext.LateBoundSymbols[new(columnName, tableAlias)] = copiedSymbol;
                if (!subQueryContext.TryAddSymbol(ident, copiedSymbol))
                {
                    throw new QueryPlanException($"Duplicate symbol for ident {ident} '{symbol}'");
                }
            }

            rewrittenPlans.Add(plan);
        }

        return rewrittenPlans;
    }

    private (List<LogicalPlan>, BaseExpression) ProcessSubQueries(
        BindContext parentContext,
        BaseExpression expression,
        List<SubQueryPlan> subQueryStatements
        )
    {
        // IFF the subqueries are uncorrelated we can bind them first. they'll be run first
        var queryPlans = new List<LogicalPlan>();
        var updatedExpression = expression;

        for (var i = 0; i < subQueryStatements.Count; i++)
        {
            var subQueryStmt = subQueryStatements[i];
            var bindContext = new BindContext
            {
                SupportsLateBinding = true,
            };

            var memRef = _catalog.OpenMemoryTable();
            var table = _bufferPool.GetMemoryTable(memRef.TableId);

            if (subQueryStmt is SubQuerySelectPlan subQueryPlan)
            {
                var subPlan = CreateLogicalPlan(subQueryPlan.Select, bindContext);
                if (subPlan.OutputSchema.Count != 1)
                {
                    if (!subQueryPlan.ExistsOnly)
                    {
                        throw new QueryPlanException("Subquery must return a single column.");
                    }
                    // These don't affect the result, just pop them off
                    if (subPlan is Distinct d)
                    {
                        subPlan = d.Input;
                    }
                    if (subPlan is Limit l)
                    {
                        subPlan = l.Input;
                    }
                    if (subPlan is Projection p)
                    {
                        subPlan = p.Input;
                        bindContext.ResetRefCounts();
                        bindContext.LateBoundSymbols.Clear();
                    }
                    else
                    {
                        throw new QueryPlanException($"Expected root of subplan to be a projection but got {subPlan.GetType().Name}");
                    }

                    // Rewrite to literal 1 top 1
                    IReadOnlyList<BaseExpression> litOne = [new BoolLiteral(true)];
                    litOne = _binder.Bind(bindContext, litOne, subPlan.OutputSchema);
                    subPlan = new Projection(subPlan, litOne, SchemaFromExpressions(litOne, null), null);
                    subPlan = new Limit(subPlan, 1);

                    subPlan = ReBindPlan(subPlan, bindContext);
                }

                var sourceCol = subPlan.OutputSchema[0];
                var newColumn = table.AddColumnToSchema(sourceCol.Name, sourceCol.DataType, "", "");

                subPlan = subPlan with
                {
                    PreBoundOutputs = [newColumn],
                    BindContext = bindContext,
                };

                var subQueryId = subQueryPlan.Expression.SubQueryId;
                var isCorrelated = bindContext.LateBoundSymbols.Any();
                MemoryStorage correlatedInputTable = default;
                if (isCorrelated)
                {
                    correlatedInputTable = _catalog.OpenMemoryTable();
                    var tuple = new Tuple<int, MemoryStorage>(subQueryId, correlatedInputTable);
                    parentContext.CorrelatedSubQueryInputs.Add(tuple);
                    bindContext.CorrelatedSubQueryInputs.Add(tuple);
                }

                subQueryStatements[i] = subQueryPlan = subQueryPlan with
                {
                    Expression = subQueryPlan.Expression with
                    {
                        Correlated = isCorrelated,
                        BoundInputMemoryTable = correlatedInputTable,
                        BoundDataType = sourceCol.DataType,
                        BoundOutputColumn = newColumn.ColumnRef,
                        BoundMemoryTable = memRef,
                    },
                };

                parentContext.AddSymbol(subQueryPlan.Expression);

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
            }
            else if (subQueryStmt is SubQueryInPlan inPlan)
            {
                var bound = (ExpressionList)_binder.Bind(parentContext, inPlan.ExpressionList, []);
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
                        IsArrayLike = true,
                    },
                };

                parentContext.AddSymbol(inPlan.Expression);

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

        return (queryPlans, updatedExpression);
    }

    public static IReadOnlyList<ColumnSchema> SchemaFromExpressions(
        IReadOnlyList<BaseExpression> expressions,
        string? selectAlias)
    {
        var schema = new List<ColumnSchema>(expressions.Count);
        foreach (var expr in expressions)
        {
            string? sourceTable = null;
            if (expr is ColumnExpression colExpr)
            {
                sourceTable = colExpr.Table;
            }

            schema.Add(new ColumnSchema(
                default,
                default,
                expr.Alias,
                expr.BoundFunction!.ReturnType,
                expr.BoundFunction!.ReturnType.ClrTypeFromDataType(),
                SourceTableName: selectAlias ?? sourceTable ?? "",
                SourceTableAlias: selectAlias ?? sourceTable ?? ""
                ));
        }

        return schema;
    }

    private LogicalPlan BindRelations(BindContext context, SelectStatement select)
    {
        List<BaseExpression> conjunctions = QueryRewriter.SplitRewriteSplitConjunctions(select.Where);

        var relations = new List<JoinedRelation>();
        foreach (var table in select.From.TableStatements)
        {
            if (table is TableStatement tableStmt)
            {
                var scan = CreateScanForTable(tableStmt);
                relations.Add(new JoinedRelation(
                    tableStmt.Alias ?? tableStmt.Table,
                    scan,
                    JoinType.Cross
                    ));
            }
            else if (table is SelectStatement selectStmt)
            {
                // TODO I might need to move binding till after all base columns from all scans
                // including nested queries have been placed into context

                var name = selectStmt.Alias ?? throw new QueryPlanException($"expression '{selectStmt}' has no alias.");
                var scan = CreateLogicalPlan(selectStmt, context);
                relations.Add(new JoinedRelation(
                    name,
                    scan,
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

                conjunctions.AddRange(QueryRewriter.SplitRewriteSplitConjunctions(join.JoinConstraint));
            }
        }

        var edges = new List<Edge>();
        var filters = new List<BaseExpression>();

        var supportsLateBinding = context.SupportsLateBinding;
        context.SupportsLateBinding = false; // In the context of joins, we don't support late binding

        foreach (var expr in conjunctions)
        {
            var found = false;
            for (var i = 0; i < relations.Count; i++)
            {
                var one = relations[i];
                // TODO do this without exceptions
                try
                {
                    _ = _binder.Bind(context, expr, one.Plan.OutputSchema, mutateContext: false);
                    var bound = _binder.Bind(context, expr, one.Plan.OutputSchema, mutateContext: true);
                    edges.Add(new UnaryEdge(one.Name, bound));
                    found = true;
                    break;
                }
                catch (QueryPlanException)
                {
                }
                catch (FunctionBindException)
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
                        _ = _binder.Bind(context, expr, mergedSchema, mutateContext: false);
                        var bound = _binder.Bind(context, expr, mergedSchema, mutateContext: true);
                        edges.Add(new BinaryEdge(one.Name, two.Name, bound));
                        found = true;
                        break;
                    }
                    catch (QueryPlanException)
                    {
                    }
                    catch (FunctionBindException)
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

        context.SupportsLateBinding = supportsLateBinding;

        if (relations.Count == 1)
        {
            var plan = relations[0].Plan;
            if (select.Where != null)
            {
                var where = _binder.Bind(context, select.Where, plan.OutputSchema);
                return new Filter(plan, where);
            }
            return plan;
        }

        var schema = GetCombinedOutputSchema(relations.Select(r => r.Plan));
        var boundFilters = filters.Select(f => _binder.Bind(context, f, schema)).ToList();
        return new JoinSet(relations, edges, boundFilters);

        Scan CreateScanForTable(TableStatement tableStmt)
        {
            var table = _catalog.GetTable(tableStmt.Table);
            context.AddSymbols(table, tableStmt.Alias);

            return new Scan(
                table.Name,
                table.Id,
                null,
                table.Columns,
                Cardinality: table.NumRows,
                Projection: false,
                // Add the table alias here so the select from a join can disambiguate
                Alias: tableStmt.Alias);
        }
    }

    public static List<ColumnSchema> AddTableAlias(IReadOnlyList<ColumnSchema> columns, string tableAlias)
    {
        return columns.Select(c => c with
        {
            SourceTableAlias = tableAlias,
        }).ToList();
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
