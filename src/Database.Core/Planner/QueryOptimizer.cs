using System.Diagnostics.CodeAnalysis;
using Database.Core.Catalog;
using Database.Core.Expressions;
using BinaryExpression = Database.Core.Expressions.BinaryExpression;

namespace Database.Core.Planner;

public class QueryOptimizer(ExpressionBinder _binder)
{
    public LogicalPlan OptimizeBlah(LogicalPlan plan)
    {
        LogicalPlan previous = plan;
        LogicalPlan updated = Optimize(previous);
        var iters = 0;
        // TODO fix this
        while (iters < 5)
        {
            iters++;
            previous = updated;
            updated = Optimize(previous);
        }

        return updated;
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

        return plan;
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

        // egh null is not correct
        if (TryPushDownFilter(filter.Input, filter.Predicate, [], out var updated))
        {
            return updated;
        }

        if (filter.Input is Join { JoinType: JoinType.Cross } join
            && filter.Predicate is BinaryExpression { Operator: TokenType.EQUAL } b)
        {
            if (TryBind(b.Left, join.Left.OutputSchema, out var _)
                && TryBind(b.Right, join.Right.OutputSchema, out var _))
            {
                return new Join(join.Left, join.Right, JoinType.Inner, filter.Predicate, join.OutputColumns);
            }
        }

        return filter;
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
