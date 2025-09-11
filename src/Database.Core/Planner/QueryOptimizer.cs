using System.Diagnostics.CodeAnalysis;
using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Expressions;
using Database.Core.Options;
using Database.Core.Planner.LogicalRules;
using Database.Core.Planner.QueryGraph;
using BinaryExpression = Database.Core.Expressions.BinaryExpression;

namespace Database.Core.Planner;

public class QueryOptimizer(ConfigOptions config, ExpressionBinder _binder, ParquetPool bufferPool)
{
    public LogicalPlan OptimizePlan(LogicalPlan plan, BindContext context)
    {
        var rule0 = new SplitConjunctionPredicateRule(config);
        plan = plan.Rewrite(p =>
        {
            if (rule0.CanRewrite(p))
            {
                return rule0.Rewrite(context, p);
            }
            return p;
        });

        var rule = new CorrelatedSubQueryRule(config, _binder);
        plan = plan.Rewrite(p =>
        {
            if (rule.CanRewrite(p))
            {
                return rule.Rewrite(context, p);
            }
            return p;
        });

        if (plan is PlanWithSubQueries planWithSub)
        {
            var optUncorrelated = new List<LogicalPlan>();
            for (var i = 0; i < planWithSub.Uncorrelated.Count; i++)
            {
                var subquery = planWithSub.Uncorrelated[i];
                var subContext = subquery.BindContext ?? throw new QueryPlanException("Subquery has no bind context");
                optUncorrelated.Add(PerformOptimizationSteps(subquery, subContext));
            }

            return planWithSub with
            {
                Uncorrelated = optUncorrelated,
                Plan = PerformOptimizationSteps(planWithSub.Plan, context),
            };
        }

        // If there are subqueries we were unable to decorrelate, optimize them individually
        plan = plan.Rewrite(p =>
        {
            if (p is Apply apply)
            {
                var children = apply.Correlated.ToList();
                var anyChanges = false;
                for (var i = 0; i < children.Count; i++)
                {
                    var subquery = children[i];
                    var subContext = subquery.BindContext ?? throw new QueryPlanException("Subquery has no bind context");
                    children[i] = PerformOptimizationSteps(subquery, subContext);
                    anyChanges = anyChanges || !ReferenceEquals(children[i], subquery);
                }

                if (!anyChanges)
                {
                    return apply;
                }
                return apply with { Correlated = children, };
            }
            return p;
        });

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

        if (plan is Apply apply)
        {
            return OptimizeApply(apply, parents, context);
        }

        throw new NotImplementedException($"Type of {plan.GetType().Name} not implemented in QueryOptimizer");
    }

    private LogicalPlan OptimizeApply(Apply apply, IReadOnlyList<LogicalPlan> parents, BindContext context)
    {
        return apply with
        {
            Input = Optimize(apply.Input, parents, context),
        };
    }

    private JoinOrder FindBestJoinOrder(JoinSet joinSet)
    {
        // Currently, this is just a brute force exhaustive search of all possible join orders
        // given by the edges in the join set. To support larger joins I'll probably want
        // to consider using IKKBZ + DPhyp
        // https://15799.courses.cs.cmu.edu/spring2025/slides/07-joins1.pdf
        // https://db.in.tum.de/~radke/papers/hugejoins.pdf

        var binaryEdges = joinSet.Edges.Where(e => e is BinaryEdge).Cast<BinaryEdge>().ToList();
        var relationsByName = joinSet.Relations.ToDictionary(r => r.Name);
        var relationToEdges = new Dictionary<JoinedRelation, List<BinaryEdge>>();

        foreach (var rel in joinSet.Relations)
        {
            if (rel.JoinType == JoinType.Inner)
            {
                relationToEdges.Add(rel, binaryEdges
                    .Where(e => e.One == rel.Name || e.Two == rel.Name)
                    .Where(e => e.Expression is BinaryExpression { Operator: TokenType.EQUAL }) // Must be an equi-join
                    .ToList()
                );
            }
        }

        var possibleJoins = new List<(JoinedRelation, JoinedRelation, BinaryEdge)>();
        foreach (var (rel, edges) in relationToEdges)
        {
            foreach (var edge in edges)
            {
                var other = edge.One == rel.Name ? edge.Two : edge.One;
                var otherRel = relationsByName[other];
                possibleJoins.Add((rel, otherRel, edge));
            }
        }

        var empty = new JoinOrder(0, [], []);
        return FindBestJoinOrder(empty, possibleJoins, new HashSet<string>());
    }

    private JoinSet PushDownUnaryEdges(JoinSet joinSet)
    {
        var unaryEdges = joinSet.Edges.Where(e => e is UnaryEdge).Cast<UnaryEdge>().ToList();
        if (unaryEdges.Count == 0)
        {
            return joinSet;
        }

        var plans = joinSet.Relations.ToDictionary(r => r.Name);
        foreach (var edge in unaryEdges)
        {
            var relation = plans[edge.Relation];
            relation = relation with
            {
                Plan = new Filter(relation.Plan, edge.Expression),
                NumRows = (long)(relation.NumRows * CostEstimation.EstimateSelectivity(edge.Expression)),
            };
            plans[edge.Relation] = relation;
        }

        return joinSet with
        {
            Relations = plans.Values.ToList(),
            Edges = joinSet.Edges.Where(e => e is not UnaryEdge).ToList(),
        };
    }

    private record JoinOrder(
        long NumRows,
        IReadOnlyList<(JoinedRelation, JoinedRelation, BinaryEdge)> Joins,
        IReadOnlyList<Tuple<IReadOnlySet<string>, LogicalPlan>> DisjoinPlans
        );

    private JoinOrder FindBestJoinOrder(
        JoinOrder graph,
        IReadOnlyList<(JoinedRelation, JoinedRelation, BinaryEdge)> possibleJoins,
        IReadOnlySet<string> inSet
        )
    {
        JoinOrder? min = null;

        foreach (var join in possibleJoins)
        {
            var (node1, node2, edge) = join;
            IReadOnlyList<(JoinedRelation, JoinedRelation, BinaryEdge)> newJoins = [.. graph.Joins, join];

            var newConnectedNodes = graph.DisjoinPlans.ToList();

            var components1 = newConnectedNodes.FirstOrDefault(c => c.Item1.Contains(node1.Name));
            var components2 = newConnectedNodes.FirstOrDefault(c => c.Item1.Contains(node2.Name));

            if (components1 == null && components2 == null)
            {
                var (left, right) = MaxLeft(node1.Plan, node2.Plan);
                var joined = new Join(left, right, JoinType.Inner, edge.Expression);
                newConnectedNodes.Add(new(new HashSet<string> { node1.Name, node2.Name }, joined));
            }
            else if (components1 != null && components2 != null)
            {
                if (components1.Equals(components2))
                {
                    // The two nodes are already connected, just skip this edge
                    continue;
                }
                // We're merging two subgraphs, so we need to remove the old ones
                newConnectedNodes.Remove(components1);
                newConnectedNodes.Remove(components2);

                var (hashset1, left1) = components1;
                var (hashset2, left2) = components2;
                (left1, left2) = MaxLeft(left1, left2);
                var joined = new Join(left1, left2, JoinType.Inner, edge.Expression);
                newConnectedNodes.Add(new(hashset1.Union(hashset2).ToHashSet(), joined));
            }
            else if (components1 != null)
            {
                // Add to existing subgraph
                newConnectedNodes.Remove(components1);

                var (hashset, left) = components1;
                var right = node2.Plan;
                (left, right) = MaxLeft(left, right);
                var joined = new Join(left, right, JoinType.Inner, edge.Expression);
                newConnectedNodes.Add(new(hashset.Union([node2.Name]).ToHashSet(), joined));
            }
            else if (components2 != null)
            {
                newConnectedNodes.Remove(components2);

                var (hashset, left) = components2;
                var right = node1.Plan;
                (left, right) = MaxLeft(left, right);
                var joined = new Join(left, right, JoinType.Inner, edge.Expression);
                newConnectedNodes.Add(new(hashset.Union([node1.Name]).ToHashSet(), joined));
            }


            var newNumRows = newConnectedNodes.Sum(g => g.Item2.NumRows);
            var newGraph = new JoinOrder(newNumRows, newJoins, newConnectedNodes);
            var remainingJoins = possibleJoins.Where(j => j != join).ToList();

            var candidate = FindBestJoinOrder(
                newGraph,
                remainingJoins,
                inSet.Union([node1.Name, node2.Name]).ToHashSet()
                );
            if (min == null || candidate.NumRows < min.NumRows)
            {
                min = candidate;
            }
        }

        if (min == null)
        {
            return graph;
        }

        return min;

        (LogicalPlan, LogicalPlan) MaxLeft(LogicalPlan l, LogicalPlan r)
        {
            return l.NumRows > r.NumRows ? (l, r) : (r, l);
        }
    }

    private LogicalPlan ExpandJoinSet(JoinSet joinSet, BindContext context)
    {
        // Push the filters down so the cardinality estimates reflect the filtered rows
        // TODO consider duplicating the filters to both sides of the join where possible
        joinSet = PushDownUnaryEdges(joinSet);
        var best = FindBestJoinOrder(joinSet);
        var plan = best.DisjoinPlans.SingleOrDefault()?.Item2;

        var detachedPlans = new List<LogicalPlan>();
        if (plan != null)
        {
            detachedPlans.Add(plan);
        }

        foreach (var rel in joinSet.Relations.Where(r => r.JoinType != JoinType.Inner))
        {
            detachedPlans.Add(rel.Plan);
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

            var usedEdges = best.Joins.Select(t => t.Item3).ToHashSet();
            var unusedEdges = joinSet.Edges.Where(e => !usedEdges.Contains(e) && e is not UnaryEdge).ToList();
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
                    Cardinality: 100, // TODO this should be calculated from the subquery
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
