using System.Diagnostics.CodeAnalysis;
using Database.Core.Catalog;
using Database.Core.Expressions;
using Database.Core.Options;
using BinaryExpression = Database.Core.Expressions.BinaryExpression;

namespace Database.Core.Planner;

public class QueryOptimizer(ConfigOptions config, ExpressionBinder _binder)
{
    public LogicalPlan OptimizePlan(LogicalPlan plan, BindContext context)
    {
        if (!config.LogicalOptimization)
        {
            return plan;
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

        throw new NotImplementedException($"Type of {plan.GetType().Name} not implemented in QueryOptimizer");
    }

    private LogicalPlan OptimizeLimit(Limit limit, IReadOnlyList<LogicalPlan> parents, BindContext context)
    {
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
        if (!config.OptJoin)
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

        // This is not correct, need to look higher up the tree
        if (filter.Input is not Scan && TryPushDownFilter(filter.Input, filter.Predicate, [], out var updated))
        {
            return updated;
        }

        return filter with
        {
            Input = Optimize(filter.Input, parents, context),
        };
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

        if (!config.OptSplitPredicates)
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

        if (!config.OptPushDownFilter)
        {
            return false;
        }

        if (plan is Scan scan)
        {
            if (parents.Any(p => p is Filter filter && filter.Predicate == predicate))
            {
                return false;
            }

            if (TryBind(predicate, scan.OutputColumns, out var boundPredicate))
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
            boundExpression = _binder.Bind(new BindContext(), expression, columns, ignoreMissingColumns: false);
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

        if (!config.OptProjectionPushDown)
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
            if (matches.Count > 0)
            {
                // Its fine, same table might be aliased in the query
                // So we're just not going to prune as much as we could
            }

            var match = matches.First();
            if (match.RefCount > 0)
            {
                result.Add(col);
            }
        }

        usedColumns = result;
        return true;
    }
}
