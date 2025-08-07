using System.Diagnostics;
using Database.Core.Catalog;
using Database.Core.Expressions;
using Database.Core.Operations;

namespace Database.Core.Planner;

public abstract record LogicalPlan()
{
    public abstract IReadOnlyList<ColumnSchema> OutputSchema { get; }
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
}

[DebuggerDisplay("filter({Predicate})")]
public record Filter(
    LogicalPlan Input,
    BaseExpression Predicate
    ) : LogicalPlan
{
    public override IReadOnlyList<ColumnSchema> OutputSchema => Input.OutputSchema;
}

[DebuggerDisplay("projection({OutputColumns})})")]
public record Projection(
    LogicalPlan Input,
    IReadOnlyList<BaseExpression> Expressions,
    IReadOnlyList<ColumnSchema> OutputColumns,
    bool AppendExpressions = false
) : LogicalPlan
{
    public override IReadOnlyList<ColumnSchema> OutputSchema => OutputColumns;
}

public record Join(
    LogicalPlan Left,
    LogicalPlan Right,
    JoinType JoinType,
    BaseExpression? Condition
) : LogicalPlan
{
    public override IReadOnlyList<ColumnSchema> OutputSchema => QueryPlanner.ExtendSchema(Left.OutputSchema, Right.OutputSchema);
}

public record Aggregate(
    LogicalPlan Input,
    IReadOnlyList<BaseExpression> GroupBy,
    IReadOnlyList<BaseExpression> Aggregates
    ) : LogicalPlan
{
    public override IReadOnlyList<ColumnSchema> OutputSchema => QueryPlanner.SchemaFromExpressions(Aggregates);
}


public record Sort(
    LogicalPlan Input,
    IReadOnlyList<OrderingExpression> OrderBy
) : LogicalPlan
{
    public override IReadOnlyList<ColumnSchema> OutputSchema => Input.OutputSchema;
}

public record Distinct(
    LogicalPlan Input
) : LogicalPlan
{
    public override IReadOnlyList<ColumnSchema> OutputSchema => Input.OutputSchema;
}

public record Limit(
    LogicalPlan Input,
    int Count
) : LogicalPlan
{
    public override IReadOnlyList<ColumnSchema> OutputSchema => Input.OutputSchema;
}
