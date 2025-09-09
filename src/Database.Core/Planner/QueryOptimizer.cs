using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks.Dataflow;
using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Expressions;
using Database.Core.Options;
using Database.Core.Planner.QueryGraph;
using BinaryExpression = Database.Core.Expressions.BinaryExpression;

namespace Database.Core.Planner;

public class QueryOptimizer(ConfigOptions config, ExpressionBinder _binder, ParquetPool bufferPool)
{
    public LogicalPlan OptimizePlan(LogicalPlan plan, BindContext context)
    {
        if (plan is PlanWithSubQueries planWithSub)
        {
            var optCorrelated = new List<LogicalPlan>();
            for (var i = 0; i < planWithSub.Correlated.Count; i++)
            {
                var subquery = planWithSub.Correlated[i];
                var subContext = subquery.BindContext ?? throw new QueryPlanException("Subquery has no bind context");
                subContext.SupportsLateBinding = false;
                optCorrelated.Add(PerformOptimizationSteps(subquery, subContext));
            }

            var optUncorrelated = new List<LogicalPlan>();
            for (var i = 0; i < planWithSub.Uncorrelated.Count; i++)
            {
                var subquery = planWithSub.Uncorrelated[i];
                var subContext = subquery.BindContext ?? throw new QueryPlanException("Subquery has no bind context");
                optUncorrelated.Add(PerformOptimizationSteps(subquery, subContext));
            }

            return planWithSub with
            {
                Correlated = optCorrelated,
                Uncorrelated = optUncorrelated,
                Plan = PerformOptimizationSteps(planWithSub.Plan, context),
            };
        }

        return PerformOptimizationSteps(plan, context);
    }

    private LogicalPlan PerformOptimizationSteps(LogicalPlan plan, BindContext context)
    {
        var maxIters = config.MaxLogicalOptimizationSteps;
        LogicalPlan previous = plan;
        LogicalPlan updated;
        var iters = 0;
        while (iters < maxIters)
        {
            iters++;
            updated = Optimize(previous, [], context);
            if (updated.Equals(previous))
            {
                break;
            }
            previous = updated;
        }

        return previous;
    }

    public List<LogicalPlan> OptimizePlanWithHistory(LogicalPlan plan, BindContext context)
    {
        var history = new List<LogicalPlan>() { plan };
        if (!config.LogicalOptimization)
        {
            return history;
        }

        var maxIters = config.MaxLogicalOptimizationSteps;
        LogicalPlan previous = plan;
        LogicalPlan updated;
        var iters = 0;
        while (iters < maxIters)
        {
            iters++;
            updated = Optimize(previous, [], context);
            if (updated.Equals(previous))
            {
                break;
            }
            previous = updated;
            history.Add(updated);
        }

        return history;
    }


    private LogicalPlan Optimize(LogicalPlan plan, IReadOnlyList<LogicalPlan> parents, BindContext context)
    {
        parents = [.. parents, plan];
        if (plan is Filter filter)
        {
            return OptimizeFilter(filter, parents, context);
        }

        if (plan is Join join)
        {
            return OptimizeJoin(join, parents, context);
        }

        if (plan is Aggregate aggregate)
        {
            return OptimizeAggregate(aggregate, parents, context);
        }

        if (plan is Projection project)
        {
            return OptimizeProjection(project, parents, context);
        }

        if (plan is Distinct distinct)
        {
            return OptimizeDistinct(distinct, parents, context);
        }

        if (plan is Sort sort)
        {
            return OptimizeSort(sort, parents, context);
        }

        if (plan is Limit limit)
        {
            return OptimizeLimit(limit, parents, context);
        }

        if (plan is Scan scan)
        {
            if (ShouldAddProjectionPushDown(scan, parents, context, out var usedColumns))
            {
                return scan with
                {
                    Projection = true,
                    OutputColumns = usedColumns,
                };
            }
            return scan;
        }

        if (plan is JoinSet joinSet)
        {
            return ExpandJoinSet(joinSet, context);
        }

        if (plan is TopNSort top)
        {
            return OptimizeTopSort(top, parents, context);
        }

        throw new NotImplementedException($"Type of {plan.GetType().Name} not implemented in QueryOptimizer");
    }

    private LogicalPlan ExpandJoinSet(JoinSet joinSet, BindContext context)
    {
        var groups = new List<List<Tuple<JoinedRelation, BinaryEdge>>>();
        var queue = new Queue<JoinedRelation>(joinSet.Relations.Where(r => r.JoinType == JoinType.Inner).ToList());
        var noMatches = new List<JoinedRelation>();

        var binaryEdges = joinSet.Edges.Where(e => e is BinaryEdge).Cast<BinaryEdge>().ToList();

        while (queue.Count > 0)
        {
            var current = queue.Dequeue();
            if (groups.Count == 0)
            {
                groups.Add(new List<Tuple<JoinedRelation, BinaryEdge>>
                {
                    new Tuple<JoinedRelation, BinaryEdge>(current, null!)
                });
                continue;
            }


            var found = false;
            foreach (var edge in binaryEdges.Where(e => current.Name == e.One || current.Name == e.Two))
            {
                var other = edge.One == current.Name ? edge.Two : edge.One;

                foreach (var group in groups)
                {
                    if (group.Any(g => g.Item1.Name == other))
                    {
                        // TODO this basic greedy algo can be improved
                        group.Add(new Tuple<JoinedRelation, BinaryEdge>(current, edge));
                        found = true;
                        break;
                    }
                }
                if (found)
                {
                    break;
                }
            }

            if (found)
            {
                foreach (var m in noMatches)
                {
                    queue.Enqueue(m);
                }
                noMatches.Clear();
            }
            else
            {
                if (queue.Count == 0)
                {
                    groups.Add([new Tuple<JoinedRelation, BinaryEdge>(current, null!)]);
                    foreach (var m in noMatches)
                    {
                        queue.Enqueue(m);
                    }
                    noMatches.Clear();
                }
                else
                {
                    noMatches.Add(current);
                }
            }
        }

        var detachedPlans = new List<LogicalPlan>();

        foreach (var rel in joinSet.Relations.Where(r => r.JoinType != JoinType.Inner))
        {
            detachedPlans.Add(rel.Plan);
        }

        foreach (var group in groups)
        {
            var plan = group.First().Item1.Plan;
            foreach (var name in group.Skip(1))
            {
                plan = new Join(plan,
                    name.Item1.Plan,
                    name.Item1.JoinType,
                    name.Item2.Expression);
            }
            detachedPlans.Add(plan);
        }

        var root = detachedPlans[0];
        for (var i = 1; i < detachedPlans.Count; i++)
        {
            root = new Join(root, detachedPlans[i], JoinType.Cross, null!);
        }
        root = AddFilters(root);
        return root;

        LogicalPlan AddFilters(LogicalPlan r)
        {
            foreach (var filter in joinSet.Filters)
            {
                r = new Filter(r, filter);
            }

            var usedEdges = groups.SelectMany(g => g.Skip(1).Select(t => t.Item2)).ToHashSet();
            var unusedEdges = joinSet.Edges.Where(e => !usedEdges.Contains(e)).ToList();
            foreach (var edge in unusedEdges)
            {
                r = new Filter(r, edge.Expression);
            }
            return r;
        }
    }

    private LogicalPlan OptimizeTopSort(TopNSort top, IReadOnlyList<LogicalPlan> parents, BindContext context)
    {
        return top with
        {
            Input = Optimize(top.Input, parents, context),
        };
    }

    private LogicalPlan OptimizeLimit(Limit limit, IReadOnlyList<LogicalPlan> parents, BindContext context)
    {
        if (config.LogicalOptimization && config.OptTopNSort)
        {
            if (limit.Input is Sort sort)
            {
                return new TopNSort(sort.Input, limit.Count, sort.OrderBy);
            }

            if (limit.Input is Projection { Input: Sort s } projection)
            {
                // Keep the projection on the outside so column drops don't matter
                var top = new TopNSort(s.Input, limit.Count, s.OrderBy);
                return projection with
                {
                    Input = top,
                };
            }

            return limit;
        }

        return limit with
        {
            Input = Optimize(limit.Input, parents, context),
        };
    }

    private LogicalPlan OptimizeSort(Sort sort, IReadOnlyList<LogicalPlan> parents, BindContext context)
    {
        return sort with
        {
            Input = Optimize(sort.Input, parents, context),
        };
    }

    private LogicalPlan OptimizeDistinct(Distinct distinct, IReadOnlyList<LogicalPlan> parents, BindContext context)
    {
        return distinct with
        {
            Input = Optimize(distinct.Input, parents, context),
        };
    }

    private LogicalPlan OptimizeAggregate(Aggregate aggregate, IReadOnlyList<LogicalPlan> parents, BindContext context)
    {
        return aggregate with
        {
            Input = Optimize(aggregate.Input, parents, context),
        };
    }

    private LogicalPlan OptimizeProjection(Projection project, IReadOnlyList<LogicalPlan> parents, BindContext context)
    {
        return project with
        {
            Input = Optimize(project.Input, parents, context),
        };
    }

    private LogicalPlan OptimizeJoin(Join join, IReadOnlyList<LogicalPlan> parents, BindContext context)
    {
        if (!config.LogicalOptimization || !config.OptJoin)
        {
            return join;
        }

        return join with
        {
            Left = Optimize(join.Left, parents, context),
            Right = Optimize(join.Right, parents, context),
        };
    }

    private LogicalPlan OptimizeFilter(Filter filter, IReadOnlyList<LogicalPlan> parents, BindContext context)
    {
        if (TrySplitPredicate(filter.Predicate, out var left, out var right))
        {
            var orgInput = filter.Input;
            var f1 = new Filter(orgInput, left);
            var f2 = new Filter(f1, right);
            return f2;
        }

        if (filter.Predicate is BinaryExpression { Operator: TokenType.IN } inOp)
        {
            if (inOp.Right is SubQueryResultExpression e)
            {
                var table = bufferPool.GetMemoryTable(e.BoundMemoryTable.TableId);
                var scan = new Scan(
                    e.Alias,
                    e.BoundMemoryTable.TableId,
                    null,
                    table.Schema,
                    Alias: e.Alias
                    );
                var joinCond = new BinaryExpression(
                    TokenType.EQUAL, "=",
                    inOp.Left,
                    new ColumnExpression(table.Schema.Single().Name, e.Alias));
                return new Join(filter.Input, scan, JoinType.Semi, joinCond);
            }
        }

        // This is not correct, need to look higher up the tree
        if (!AllFiltersOrScan(filter.Input)
            && TryPushDownFilter(filter.Input, filter.Predicate, [], out var updated))
        {
            return updated;
        }

        return filter with
        {
            Input = Optimize(filter.Input, parents, context),
        };

        bool AllFiltersOrScan(LogicalPlan root)
        {
            var allFilters = true;
            root.Walk(p =>
            {
                if (p is not Filter && p is not Scan)
                {
                    allFilters = false;
                }
            });
            return allFilters;
        }
    }

    private bool TryCreateInnerJoin(
        LogicalPlan l,
        LogicalPlan r,
        BinaryExpression binExpr,
        [NotNullWhen(true)] out Join? innerJoin)
    {
        innerJoin = null;

        if (TryBind(binExpr.Left, l.OutputSchema, out var _)
            && TryBind(binExpr.Right, r.OutputSchema, out var _))
        {
            innerJoin = new Join(
                l,
                r,
                JoinType.Inner,
                binExpr
            );
            return true;
        }

        if (TryBind(binExpr.Left, r.OutputSchema, out var _)
            && TryBind(binExpr.Right, l.OutputSchema, out var _))
        {
            innerJoin = new Join(
                r,
                l,
                JoinType.Inner,
                binExpr
            );
            return true;
        }

        return false;
    }

    private bool TrySplitPredicate(
        BaseExpression predicate,
        [NotNullWhen(true)] out BaseExpression? left,
        [NotNullWhen(true)] out BaseExpression? right)
    {
        left = null;
        right = null;

        if (!config.LogicalOptimization || !config.OptSplitPredicates)
        {
            return false;
        }

        if (predicate is BinaryExpression { Operator: TokenType.AND } b)
        {
            left = b.Left;
            right = b.Right;
            return true;
        }

        return false;
    }

    private bool TryPushDownFilter(
        LogicalPlan plan,
        BaseExpression predicate,
        IReadOnlyList<LogicalPlan> parents,
        [NotNullWhen(true)] out LogicalPlan? updated)
    {
        updated = null;

        if (!config.LogicalOptimization || !config.OptPushDownFilter)
        {
            return false;
        }

        if (plan is Scan scan)
        {
            if (parents.Any(p => p is Filter filter && filter.Predicate == predicate))
            {
                return false;
            }

            if (TryBind(predicate, scan.OutputSchema, out var boundPredicate))
            {
                updated = new Filter(scan, boundPredicate);
                return true;
            }
        }

        if (plan is Filter filter2)
        {
            if (TryPushDownFilter(filter2.Input, predicate, Append(parents, filter2), out var updated2))
            {
                updated = filter2 with
                {
                    Input = updated2,
                };
                return true;
            }
        }

        if (plan is Join join)
        {
            var any = false;
            var left = join.Left;
            if (TryPushDownFilter(left, predicate, Append(parents, join), out var updatedLeft))
            {
                left = updatedLeft;
                any = true;
            }
            var right = join.Right;
            if (TryPushDownFilter(right, predicate, Append(parents, join), out var updatedRight))
            {
                right = updatedRight;
                any = true;
            }

            if (any)
            {
                updated = join with
                {
                    Left = left,
                    Right = right
                };
                return true;
            }

            if (plan is Join { JoinType: JoinType.Cross } join1
                && predicate is BinaryExpression { Operator: TokenType.EQUAL } binExpr)
            {
                if (TryCreateInnerJoin(join1.Left, join1.Right, binExpr, out var innerJoin))
                {
                    updated = innerJoin;
                    return true;
                }
            }
        }

        if (plan is Projection projection)
        {
            if (TryPushDownFilter(projection.Input, predicate, Append(parents, projection), out updated))
            {
                updated = projection with
                {
                    Input = updated,
                };
                return true;
            }
        }

        return false;
    }

    private static List<LogicalPlan> Append(IReadOnlyList<LogicalPlan> parents, LogicalPlan plan)
    {
        var plans = new List<LogicalPlan>(parents) { plan };
        return plans;
    }

    private bool TryBind(
        BaseExpression expression,
        IReadOnlyList<ColumnSchema> columns,
        [NotNullWhen(true)] out BaseExpression? boundExpression)
    {
        try
        {
            boundExpression = _binder.Bind(new BindContext(), expression, columns, ignoreMissingColumns: false, mutateContext: false);
            return true;
        }
        catch (Exception e)
        {
            boundExpression = null;
            return false;
        }
    }

    private bool ShouldAddProjectionPushDown(
        Scan scan,
        IReadOnlyList<LogicalPlan> parents,
        BindContext context,
        [NotNullWhen(true)] out IReadOnlyList<ColumnSchema>? usedColumns)
    {
        usedColumns = null;
        if (scan.Projection || scan.OutputSchema.Count == 1 || parents.Count <= 1)
        {
            return false;
        }

        if (!config.LogicalOptimization || !config.OptProjectionPushDown)
        {
            return false;
        }

        var result = new List<ColumnSchema>();
        var allSymbols = context.BoundSymbols.Values.Distinct().ToList();
        foreach (var col in scan.OutputSchema)
        {
            var matches = allSymbols.Where(s =>
                s.ColumnRef == col.ColumnRef && s.TableName == scan.Table
                ).ToList();

            // We're not pruning as much as we could here, but its fine.
            // This could be made slightly faster if we had better ref counting
            if (matches.Any(m => m.RefCount > 0))
            {
                result.Add(col);
            }
        }

        usedColumns = result;
        return true;
    }
}
