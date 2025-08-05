using Database.Core.Catalog;
using Database.Core.Expressions;

namespace Database.Core.Planner;

public class ExplainQuery
{
    public string Explain(LogicalPlan plan)
    {
        if (plan is Filter filter)
        {
            return $"Filter({filter.Predicate}) -> {OutputColumns(filter.OutputColumns)}" + '\n' + Explain(filter.Input);
        }

        if (plan is Join join)
        {
            var joinCon = join.Condition != null ? " on " + join.Condition : "";
            return $"Join({join.JoinType}{joinCon})" + '\n' + Explain(join.Left) + '\n' + Explain(join.Right) + "\n -> " +
                   OutputColumns(join.OutputColumns);
        }

        if (plan is Aggregate aggregate)
        {
            return $"Agg({Expressions(aggregate.Aggregates)}) group by ({Expressions(aggregate.GroupBy)})" + "\n-> " + OutputColumns(aggregate.OutputColumns) + '\n' + Explain(aggregate.Input);
        }

        if (plan is Projection project)
        {
            return $"Project({Expressions(project.Expressions)}) -> {OutputColumns(project.OutputColumns)}" + '\n' + Explain(project.Input);
        }

        if (plan is Distinct distinct)
        {
            return $"Distinct -> {OutputColumns(distinct.OutputColumns)}" + '\n' + Explain(distinct.Input);
        }

        if (plan is Sort sort)
        {
            return $"Sort({Expressions(sort.OrderBy)}) -> {OutputColumns(sort.OutputColumns)}" + '\n' + Explain(sort.Input);
        }

        if (plan is Scan scan)
        {
            return $"Scan({scan.Table}) -> {OutputColumns(scan.OutputColumns)}";
        }

        throw new NotImplementedException("Explain not implemented for this plan: {" + plan + "}");
    }

    private string OutputColumns(IReadOnlyList<ColumnSchema> columns)
    {
        return string.Join(", ", columns.Select(c => c.SourceTableAlias + "." + c.Name));
    }

    private string Expressions(IReadOnlyList<BaseExpression> expressions)
    {
        return string.Join(", ", expressions.Select(e => e.ToString()));
    }
}
