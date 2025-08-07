using System.Diagnostics.CodeAnalysis;
using Database.Core.Catalog;
using Database.Core.Expressions;
using BinaryExpression = Database.Core.Expressions.BinaryExpression;

namespace Database.Core.Planner;

public class QueryOptimizer(ExpressionBinder _binder)
{
    public LogicalPlan OptimizePlan(LogicalPlan plan, BindContext context, int maxIters = 10)
    {
        LogicalPlan previous = plan;
        LogicalPlan updated;
        var iters = 0;
        while (iters < maxIters)
        {
            iters++;
            updated = Optimize(previous, context);
            if (updated.Equals(previous))
            {
                break;
            }
            previous = updated;
        }

        return previous;
    }

    public List<LogicalPlan> OptimizePlanWithHistory(LogicalPlan plan, BindContext context, int maxIters = 10)
    {
        var history = new List<LogicalPlan>() { plan };
        LogicalPlan previous = plan;
        LogicalPlan updated;
        var iters = 0;
        while (iters < maxIters)
        {
            iters++;
            updated = Optimize(previous, context);
            if (updated.Equals(previous))
            {
                break;
            }
            previous = updated;
            history.Add(updated);
        }

        return history;
    }


    private LogicalPlan Optimize(LogicalPlan plan, BindContext context)
    {
        if (plan is Filter filter)
        {
            return OptimizeFilter(filter, context);
        }

        if (plan is Join join)
        {
            return OptimizeJoin(join, context, context);
        }

        if (plan is Aggregate aggregate)
        {
            return OptimizeAggregate(aggregate, context);
        }

        if (plan is Projection project)
        {
            return OptimizeProjection(project, context);
        }

        if (plan is Distinct distinct)
        {
            return OptimizeDistinct(distinct, context);
        }

        if (plan is Sort sort)
        {
            return OptimizeSort(sort, context);
        }

        if (plan is Limit limit)
        {
            return OptimizeLimit(limit, context);
        }

        if (plan is Scan scan)
        {
            return scan;
        }

        throw new NotImplementedException($"Type of {plan.GetType().Name} not implemented in QueryOptimizer");
    }

    private LogicalPlan OptimizeLimit(Limit limit, BindContext context)
    {
        return limit with
        {
            Input = Optimize(limit.Input, context),
        };
    }

    private LogicalPlan OptimizeSort(Sort sort, BindContext context)
    {
        return sort with
        {
            Input = Optimize(sort.Input, context),
        };
    }

    private LogicalPlan OptimizeDistinct(Distinct distinct, BindContext context)
    {
        return distinct with
        {
            Input = Optimize(distinct.Input, context),
        };
    }

    private LogicalPlan OptimizeAggregate(Aggregate aggregate, BindContext context)
    {
        return aggregate with
        {
            Input = Optimize(aggregate.Input, context),
        };
    }

    private LogicalPlan OptimizeProjection(Projection project, BindContext context)
    {
        return project with
        {
            Input = Optimize(project.Input, context),
        };
    }

    private LogicalPlan OptimizeJoin(Join join, BindContext context, BindContext context1)
    {
        return join with
        {
            Left = Optimize(join.Left, context),
            Right = Optimize(join.Right, context1),
        };
    }

    private LogicalPlan OptimizeFilter(Filter filter, BindContext context)
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
            Input = Optimize(filter.Input, context),
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
        if (predicate is BinaryExpression { Operator: TokenType.AND } b)
        {
            left = b.Left;
            right = b.Right;
            return true;
        }

        left = null;
        right = null;
        return false;
    }

    private bool TryPushDownFilter(
        LogicalPlan plan,
        BaseExpression predicate,
        IReadOnlyList<LogicalPlan> parents,
        [NotNullWhen(true)] out LogicalPlan? updated)
    {
        updated = null;

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
}
