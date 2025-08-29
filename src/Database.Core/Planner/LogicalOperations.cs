using System.Diagnostics;
using Database.Core.Catalog;
using Database.Core.Expressions;
using Database.Core.Operations;
using Database.Core.Planner.QueryGraph;

namespace Database.Core.Planner;

public abstract record LogicalPlan
{
    public int PlanId { get; init; }

    // Only set on the roots?
    public BindContext? BindContext { get; init; }

    // For subqueries
    public IReadOnlyList<ColumnSchema> PreBoundOutputs { get; set; } = [];

    public abstract IReadOnlyList<ColumnSchema> OutputSchema { get; }

    public abstract IEnumerable<LogicalPlan> Inputs();

    protected abstract LogicalPlan WithInputs(IReadOnlyList<LogicalPlan> newInputs);

    public LogicalPlan Rewrite(Func<LogicalPlan, LogicalPlan?> rewriter)
    {
        var replaced = rewriter(this);
        if (replaced is not null && !ReferenceEquals(replaced, this))
        {
            // If this node is replaced, continue rewriting inside the replacement
            return replaced.Rewrite(rewriter);
        }

        var existingChildren = Inputs().ToArray();
        var newChildren = new LogicalPlan[existingChildren.Length];
        var anyChanged = false;
        for (var i = 0; i < existingChildren.Length; i++)
        {
            var rewrittenChild = existingChildren[i].Rewrite(rewriter);
            newChildren[i] = rewrittenChild;
            if (!ReferenceEquals(rewrittenChild, existingChildren[i]))
            {
                anyChanged = true;
            }
        }

        return anyChanged ? WithInputs(newChildren) : this;
    }

    public void Walk(Action<LogicalPlan> fun)
    {
        fun(this);
        foreach (var child in Inputs())
        {
            child.Walk(fun);
        }
    }
}

[DebuggerDisplay("plan({PlanId}) with subqueries(c={Correlated.Count}, u={Uncorrelated.Count})")]
public record PlanWithSubQueries(
    LogicalPlan Plan,
    List<LogicalPlan> Correlated,
    List<LogicalPlan> Uncorrelated
    ) : LogicalPlan
{
    public override IReadOnlyList<ColumnSchema> OutputSchema => Plan.OutputSchema;

    public override IEnumerable<LogicalPlan> Inputs()
    {
        yield return Plan;
        foreach (var sub in Uncorrelated)
        {
            yield return sub;
        }
    }

    protected override LogicalPlan WithInputs(IReadOnlyList<LogicalPlan> newInputs)
    {
        var n = Uncorrelated.Count + 1;
        if (newInputs.Count != n)
        {
            throw new ArgumentException($"PlanWithSubQueries expects {n} children but received {newInputs.Count}.");
        }
        return this with { Plan = newInputs[0], Uncorrelated = [.. newInputs.Skip(1)] };
    }
}

[DebuggerDisplay("scan({Table})")]
public record Scan(
    string Table,
    TableId TableId,
    BaseExpression? Filter,
    IReadOnlyList<ColumnSchema> OutputColumns,
    bool Projection = false,
    string? Alias = null) : LogicalPlan
{
    public override IReadOnlyList<ColumnSchema> OutputSchema => OutputColumns;

    public override IEnumerable<LogicalPlan> Inputs()
    {
        yield break;
    }

    protected override LogicalPlan WithInputs(IReadOnlyList<LogicalPlan> newInputs)
    {
        throw new NotImplementedException();
    }
}

[DebuggerDisplay("filter({Predicate})")]
public record Filter(
    LogicalPlan Input,
    BaseExpression Predicate
    ) : LogicalPlan
{
    public override IReadOnlyList<ColumnSchema> OutputSchema => Input.OutputSchema;

    public override IEnumerable<LogicalPlan> Inputs()
    {
        yield return Input;
    }

    protected override LogicalPlan WithInputs(IReadOnlyList<LogicalPlan> newInputs)
    {
        if (newInputs.Count != 1)
        {
            throw new ArgumentException($"Filter expects 1 child but received {newInputs.Count}.");
        }
        return this with { Input = newInputs[0] };
    }
}

[DebuggerDisplay("projection({OutputColumns})})")]
public record Projection(
    LogicalPlan Input,
    IReadOnlyList<BaseExpression> Expressions,
    IReadOnlyList<ColumnSchema> OutputColumns,
    string? Alias,
    bool AppendExpressions = false
) : LogicalPlan
{
    public override IReadOnlyList<ColumnSchema> OutputSchema => OutputColumns;

    public override IEnumerable<LogicalPlan> Inputs()
    {
        yield return Input;
    }

    protected override LogicalPlan WithInputs(IReadOnlyList<LogicalPlan> newInputs)
    {
        if (newInputs.Count != 1)
        {
            throw new ArgumentException($"Projection expects 1 child but received {newInputs.Count}.");
        }
        return this with { Input = newInputs[0] };
    }
}

public record JoinedRelation(string Name, LogicalPlan Plan, JoinType JoinType);

public record JoinSet(
    IReadOnlyList<JoinedRelation> Relations,
    IReadOnlyList<Edge> Edges,
    IReadOnlyList<BaseExpression> Filters
) : LogicalPlan
{
    public override IReadOnlyList<ColumnSchema> OutputSchema
    {
        get { return QueryPlanner.GetCombinedOutputSchema(Relations.Select(r => r.Plan)); }
    }

    public override IEnumerable<LogicalPlan> Inputs()
    {
        foreach (var relation in Relations)
        {
            yield return relation.Plan;
        }
    }

    protected override LogicalPlan WithInputs(IReadOnlyList<LogicalPlan> newInputs)
    {
        if (newInputs.Count != Relations.Count)
        {
            throw new ArgumentException($"JoinSet expects {Relations.Count} children but received {newInputs.Count}.");
        }
        return this with { Relations = newInputs.Select((p, i) => Relations[i] with { Plan = p }).ToList() };
    }
}

public record Join(
    LogicalPlan Left,
    LogicalPlan Right,
    JoinType JoinType,
    BaseExpression? Condition
) : LogicalPlan
{
    public override IReadOnlyList<ColumnSchema> OutputSchema => QueryPlanner.ExtendSchema(Left.OutputSchema, Right.OutputSchema);

    public override IEnumerable<LogicalPlan> Inputs()
    {
        yield return Left;
        yield return Right;
    }

    protected override LogicalPlan WithInputs(IReadOnlyList<LogicalPlan> newInputs)
    {
        if (newInputs.Count != 2)
        {
            throw new ArgumentException($"Join expects 2 children but received {newInputs.Count}.");
        }
        return this with { Left = newInputs[0], Right = newInputs[1] };
    }
}

public record Aggregate(
    LogicalPlan Input,
    IReadOnlyList<BaseExpression> GroupBy,
    IReadOnlyList<BaseExpression> Aggregates,
    string? Alias
    ) : LogicalPlan
{
    public override IReadOnlyList<ColumnSchema> OutputSchema => QueryPlanner.SchemaFromExpressions(Aggregates, Alias);

    public override IEnumerable<LogicalPlan> Inputs()
    {
        yield return Input;
    }

    protected override LogicalPlan WithInputs(IReadOnlyList<LogicalPlan> newInputs)
    {
        if (newInputs.Count != 1)
        {
            throw new ArgumentException($"Aggregate expects 1 child but received {newInputs.Count}.");
        }
        return this with { Input = newInputs[0] };
    }
}


public record Sort(
    LogicalPlan Input,
    IReadOnlyList<OrderingExpression> OrderBy
) : LogicalPlan
{
    public override IReadOnlyList<ColumnSchema> OutputSchema => Input.OutputSchema;

    public override IEnumerable<LogicalPlan> Inputs()
    {
        yield return Input;
    }

    protected override LogicalPlan WithInputs(IReadOnlyList<LogicalPlan> newInputs)
    {
        if (newInputs.Count != 1)
        {
            throw new ArgumentException($"Sort expects 1 child but received {newInputs.Count}.");
        }
        return this with { Input = newInputs[0] };
    }
}

public record Distinct(
    LogicalPlan Input
) : LogicalPlan
{
    public override IReadOnlyList<ColumnSchema> OutputSchema => Input.OutputSchema;

    public override IEnumerable<LogicalPlan> Inputs()
    {
        yield return Input;
    }

    protected override LogicalPlan WithInputs(IReadOnlyList<LogicalPlan> newInputs)
    {
        if (newInputs.Count != 1)
        {
            throw new ArgumentException($"Distinct expects 1 child but received {newInputs.Count}.");
        }
        return this with { Input = newInputs[0] };
    }
}

public record Limit(
    LogicalPlan Input,
    int Count
) : LogicalPlan
{
    public override IReadOnlyList<ColumnSchema> OutputSchema => Input.OutputSchema;

    public override IEnumerable<LogicalPlan> Inputs()
    {
        yield return Input;
    }

    protected override LogicalPlan WithInputs(IReadOnlyList<LogicalPlan> newInputs)
    {
        if (newInputs.Count != 1)
        {
            throw new ArgumentException($"Limit expects 1 child but received {newInputs.Count}.");
        }
        return this with { Input = newInputs[0] };
    }
}
