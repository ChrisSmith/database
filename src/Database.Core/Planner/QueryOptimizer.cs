using System.Diagnostics.CodeAnalysis;
using Database.Core.Catalog;
using Database.Core.Expressions;
using BinaryExpression = Database.Core.Expressions.BinaryExpression;

namespace Database.Core.Planner;

public class QueryOptimizer(ExpressionBinder _binder)
{
    public LogicalPlan OptimizePlan(LogicalPlan plan, int maxIters = 10)
    {
        LogicalPlan previous = plan;
        LogicalPlan updated;
        var iters = 0;
        while (iters < maxIters)
        {
            iters++;
            updated = Optimize(previous);
            if (updated.Equals(previous))
            {
                break;
            }
            previous = updated;
        }

        return previous;
    }

    public List<LogicalPlan> OptimizePlanWithHistory(LogicalPlan plan, int maxIters = 10)
    {
        var history = new List<LogicalPlan>() { plan };
        LogicalPlan previous = plan;
        LogicalPlan updated;
        var iters = 0;
        while (iters < maxIters)
        {
            iters++;
            updated = Optimize(previous);
            if (updated.Equals(previous))
            {
                break;
            }
            previous = updated;
            history.Add(updated);
        }

        return history;
    }


    private LogicalPlan Optimize(LogicalPlan plan)
    {
        if (plan is Filter filter)
        {
            return OptimizeFilter(filter);
        }

        if (plan is Join join)
        {
            return OptimizeJoin(join);
        }

        if (plan is Aggregate aggregate)
        {
            return OptimizeAggregate(aggregate);
        }

        if (plan is Projection project)
        {
            return OptimizeProjection(project);
        }

        if (plan is Distinct distinct)
        {
            return OptimizeDistinct(distinct);
        }

        if (plan is Sort sort)
        {
            return OptimizeSort(sort);
        }

        if (plan is Limit limit)
        {
            return OptimizeLimit(limit);
        }

        if (plan is Scan scan)
        {
            return scan;
        }

        throw new NotImplementedException($"Type of {plan.GetType().Name} not implemented in QueryOptimizer");
    }

    private LogicalPlan OptimizeLimit(Limit limit)
    {
        return limit with
        {
            Input = Optimize(limit.Input),
        };
    }

    private LogicalPlan OptimizeSort(Sort sort)
    {
        return sort with
        {
            Input = Optimize(sort.Input),
        };
    }

    private LogicalPlan OptimizeDistinct(Distinct distinct)
    {
        return distinct with
        {
            Input = Optimize(distinct.Input),
        };
    }

    private LogicalPlan OptimizeAggregate(Aggregate aggregate)
    {
        return aggregate with
        {
            Input = Optimize(aggregate.Input),
        };
    }

    private LogicalPlan OptimizeProjection(Projection project)
    {
        return project with
        {
            Input = Optimize(project.Input),
        };
    }

    private LogicalPlan OptimizeJoin(Join join)
    {
        return join with
        {
            Left = Optimize(join.Left),
            Right = Optimize(join.Right),
        };
    }

    private LogicalPlan OptimizeFilter(Filter filter)
    {
        if (TrySplitPredicate(filter.Predicate, out var left, out var right))
        {
            var orgInput = filter.Input;
            var f1 = new Filter(orgInput, left, filter.OutputColumns);
            var f2 = new Filter(f1, right, f1.OutputColumns);
            return f2;
        }

        // This is not correct, need to look higher up the tree
        if (filter.Input is not Scan && TryPushDownFilter(filter.Input, filter.Predicate, [], out var updated))
        {
            return updated;
        }



        return filter with
        {
            Input = Optimize(filter.Input),
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
            var schema = QueryPlanner.ExtendSchema(l.OutputSchema, r.OutputSchema);
            innerJoin = new Join(
                l,
                r,
                JoinType.Inner,
                binExpr,
                schema
            );
            return true;
        }

        if (TryBind(binExpr.Left, r.OutputSchema, out var _)
            && TryBind(binExpr.Right, l.OutputSchema, out var _))
        {
            var schema = QueryPlanner.ExtendSchema(r.OutputSchema, l.OutputSchema);
            innerJoin = new Join(
                r,
                l,
                JoinType.Inner,
                binExpr,
                schema
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
                updated = new Filter(scan, boundPredicate, scan.OutputColumns);
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
            boundExpression = _binder.Bind(expression, columns, ignoreMissingColumns: false);
            return true;
        }
        catch (Exception e)
        {
            boundExpression = null;
            return false;
        }
    }
}
