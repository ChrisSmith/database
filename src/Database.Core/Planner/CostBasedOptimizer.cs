using Database.Core.Expressions;
using Database.Core.Operations;
using Database.Core.Options;

namespace Database.Core.Planner;

public class CostBasedOptimizer(ConfigOptions config, PhysicalPlanner physicalPlanner)
{
    public IOperation OptimizeAndLower(LogicalPlan plan, BindContext context)
    {
        var bestPlan = SearchForBestPlan(plan, context);
        return physicalPlanner.CreatePhysicalPlan(bestPlan, context);
    }

    private LogicalPlan SearchForBestPlan(LogicalPlan plan, BindContext context)
    {
        if (!config.CostBasedOptimization)
        {
            return plan;
        }

        var bestPlan = plan switch
        {
            Filter filter => OptimizeFilter(filter, context),
            Join join => OptimizeJoin(join, context),
            Aggregate aggregate => OptimizeAggregate(aggregate, context),
            Projection project => OptimizeProjection(project, context),
            Distinct distinct => OptimizeDistinct(distinct, context),
            Sort sort => OptimizeSort(sort, context),
            Limit limit => OptimizeLimit(limit, context),
            Scan scan => scan,
            _ => throw new NotImplementedException($"Type of {plan.GetType().Name} not implemented in QueryOptimizer")
        };

        return bestPlan;
    }

    private LogicalPlan OptimizeFilter(Filter filter, BindContext context)
    {
        return filter with { Input = SearchForBestPlan(filter.Input, context) };
    }

    private LogicalPlan OptimizeAggregate(Aggregate aggregate, BindContext context)
    {
        return aggregate with { Input = SearchForBestPlan(aggregate.Input, context) };
    }

    private LogicalPlan OptimizeProjection(Projection projection, BindContext context)
    {
        return projection with { Input = SearchForBestPlan(projection.Input, context) };
    }

    private LogicalPlan OptimizeDistinct(Distinct distinct, BindContext context)
    {
        return distinct with { Input = SearchForBestPlan(distinct.Input, context) };
    }

    private LogicalPlan OptimizeSort(Sort sort, BindContext context)
    {
        return sort with { Input = SearchForBestPlan(sort.Input, context) };
    }

    private LogicalPlan OptimizeLimit(Limit limit, BindContext context)
    {
        return limit with { Input = SearchForBestPlan(limit.Input, context) };
    }

    private LogicalPlan OptimizeJoin(Join join, BindContext context)
    {
        var left = SearchForBestPlan(join.Left, context);
        var right = SearchForBestPlan(join.Right, context);

        var original = join with { Left = left, Right = right };

        if (join.JoinType == JoinType.Left || join.JoinType == JoinType.Right)
        {
            return original;
        }

        if (join.Condition is not BinaryExpression b)
        {
            return original;
        }

        // TODO the tree structure harms the optimizations here
        var swapped = new Join(
            right,
            left,
            join.JoinType,
            b with
            {
                Left = b.Right,
                Right = b.Left,
            }
        );

        // TODO don't throw away the physical plan, allow it to be passed
        // back into CreatePhysicalPlan for the parent operation
        var ogCost = physicalPlanner.CreatePhysicalPlan(original, context).EstimateCost();
        var swappedCost = physicalPlanner.CreatePhysicalPlan(swapped, context).EstimateCost();

        var sC = swappedCost.TotalCost();
        var oC = ogCost.TotalCost();
        if (sC < oC)
        {
            return swapped;
        }
        return original;
    }
}
