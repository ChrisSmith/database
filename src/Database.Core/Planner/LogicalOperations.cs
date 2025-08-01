using System.Diagnostics;
using Database.Core.Catalog;
using Database.Core.Expressions;
using Database.Core.Operations;

namespace Database.Core.Planner;

public abstract record LogicalPlan(IOperation? BoundOperation = null)
{
    public abstract IReadOnlyList<ColumnSchema> OutputSchema { get; }
}

[DebuggerDisplay("scan({Table})")]
public record Scan(
    string Table,
    TableId TableId,
    IReadOnlyList<ColumnSchema> OutputColumns,
    string? Alias = null) : LogicalPlan
{
    public override IReadOnlyList<ColumnSchema> OutputSchema => OutputColumns;
}

[DebuggerDisplay("filter({Predicate})")]
public record Filter(
    LogicalPlan Input,
    BaseExpression Predicate,
    IReadOnlyList<ColumnSchema> OutputColumns
    ) : LogicalPlan
{
    public override IReadOnlyList<ColumnSchema> OutputSchema => OutputColumns;
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
    BaseExpression Condition,
    IReadOnlyList<ColumnSchema> OutputColumns
) : LogicalPlan
{
    public override IReadOnlyList<ColumnSchema> OutputSchema => OutputColumns;
}

public record Aggregate(
    LogicalPlan Input,
    IReadOnlyList<BaseExpression> GroupBy,
    IReadOnlyList<BaseExpression> Aggregates,
    IReadOnlyList<ColumnSchema> OutputColumns
    ) : LogicalPlan
{
    public override IReadOnlyList<ColumnSchema> OutputSchema => OutputColumns;
}


public record Sort(
    LogicalPlan Input,
    IReadOnlyList<BaseExpression> OrderBy,
    IReadOnlyList<ColumnSchema> OutputColumns
) : LogicalPlan
{
    public override IReadOnlyList<ColumnSchema> OutputSchema => OutputColumns;
}

public record Distinct(
    LogicalPlan Input,
    IReadOnlyList<ColumnSchema> OutputColumns
) : LogicalPlan
{
    public override IReadOnlyList<ColumnSchema> OutputSchema => OutputColumns;
}
