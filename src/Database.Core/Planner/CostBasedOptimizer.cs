using Database.Core.Expressions;
using Database.Core.Operations;

namespace Database.Core.Planner;

public class CostBasedOptimizer(PhysicalPlanner physicalPlanner)
{
    public IOperation OptimizeAndLower(LogicalPlan plan)
    {
        var bestPlan = SearchForBestPlan(plan);
        return physicalPlanner.CreatePhysicalPlan(bestPlan);
    }

    public LogicalPlan SearchForBestPlan(LogicalPlan plan)
    {
        var bestPlan = plan switch
        {
            Filter filter => OptimizeFilter(filter),
            Join join => OptimizeJoin(join),
            Aggregate aggregate => OptimizeAggregate(aggregate),
            Projection project => OptimizeProjection(project),
            Distinct distinct => OptimizeDistinct(distinct),
            Sort sort => OptimizeSort(sort),
            Limit limit => OptimizeLimit(limit),
            Scan scan => scan,
            _ => throw new NotImplementedException($"Type of {plan.GetType().Name} not implemented in QueryOptimizer")
        };

        return bestPlan;
    }

    private LogicalPlan OptimizeFilter(Filter filter)
    {
        return filter with { Input = SearchForBestPlan(filter.Input) };
    }

    private LogicalPlan OptimizeAggregate(Aggregate aggregate)
    {
        return aggregate with { Input = SearchForBestPlan(aggregate.Input) };
    }

    private LogicalPlan OptimizeProjection(Projection projection)
    {
        return projection with { Input = SearchForBestPlan(projection.Input) };
    }

    private LogicalPlan OptimizeDistinct(Distinct distinct)
    {
        return distinct with { Input = SearchForBestPlan(distinct.Input) };
    }

    private LogicalPlan OptimizeSort(Sort sort)
    {
        return sort with { Input = SearchForBestPlan(sort.Input) };
    }

    private LogicalPlan OptimizeLimit(Limit limit)
    {
        return limit with { Input = SearchForBestPlan(limit.Input) };
    }

    private LogicalPlan OptimizeJoin(Join join)
    {
        var left = SearchForBestPlan(join.Left);
        var right = SearchForBestPlan(join.Right);

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
            },
            QueryPlanner.ExtendSchema(right.OutputSchema, left.OutputSchema)
        );

        // TODO don't throw away the physical plan, allow it to be passed
        // back into CreatePhysicalPlan for the parent operation
        var ogCost = physicalPlanner.CreatePhysicalPlan(original).EstimateCost();
        var swappedCost = physicalPlanner.CreatePhysicalPlan(swapped).EstimateCost();

        var sC = swappedCost.TotalCost();
        var oC = ogCost.TotalCost();
        if (sC < oC)
        {
            return swapped;
        }
        return original;
    }
}
