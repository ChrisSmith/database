namespace Database.Core.Planner;

public partial class QueryPlanner
{
    public LogicalPlan ReBindPlan(LogicalPlan plan, BindContext context)
    {
        return plan.Rewrite(p => Bind(p, context));
    }

    private LogicalPlan Bind(LogicalPlan plan, BindContext context)
    {
        _ = plan switch
        {
            Filter filter => BindFilter(filter, context),
            Join join => BindJoin(join, context),
            Aggregate aggregate => BindAggregate(aggregate, context),
            Projection project => BindProjection(project, context),
            Sort sort => BindSort(sort, context),
            TopNSort top => BindTopSort(top, context),
            Scan scan => BindScan(scan, context),
            Distinct distinct => distinct,
            Limit limit => limit,
        };

        // For now just bind, but don't mutate the result
        return plan;
    }

    private LogicalPlan BindScan(Scan scan, BindContext context)
    {
        if (scan.Filter != null)
        {
            return scan with
            {
                Filter = _binder.Bind(context, scan.Filter, scan.OutputColumns),
            };
        }
        return scan;
    }

    private LogicalPlan BindTopSort(TopNSort top, BindContext context)
    {
        return top with
        {
            OrderBy = _binder.Bind(context, top.OrderBy, top.Input.OutputSchema)
        };
    }

    private LogicalPlan BindSort(Sort sort, BindContext context)
    {
        return sort with
        {
            OrderBy = _binder.Bind(context, sort.OrderBy, sort.Input.OutputSchema),
        };
    }

    private LogicalPlan BindProjection(Projection project, BindContext context)
    {
        return project with
        {
            Expressions = _binder.Bind(context, project.Expressions, project.Input.OutputSchema),
        };
    }

    private LogicalPlan BindAggregate(Aggregate aggregate, BindContext context)
    {
        return aggregate with
        {
            Aggregates = _binder.Bind(context, aggregate.Aggregates, aggregate.Input.OutputSchema),
            GroupBy = _binder.Bind(context, aggregate.GroupBy, aggregate.Input.OutputSchema),
        };
    }

    private LogicalPlan BindJoin(Join join, BindContext context)
    {
        return join with
        {
            Condition = join.Condition != null ? _binder.Bind(context, join.Condition, join.Left.OutputSchema) : null,
        };
    }

    private LogicalPlan BindFilter(Filter filter, BindContext context)
    {
        return filter with
        {
            Predicate = _binder.Bind(context, filter.Predicate, filter.Input.OutputSchema),
        };
    }
}
