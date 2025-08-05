using Database.Core.Catalog;
using Database.Core.Expressions;

namespace Database.Core.Planner;

public class ExplainQuery(bool IncludeOutputColumns = true, string IdentString = "  ")
{
    public string Explain(LogicalPlan plan)
    {
        var writer = new StringWriter();
        Explain(plan, writer, 0);
        return writer.ToString();
    }

    public void Explain(LogicalPlan plan, StringWriter writer, int ident)
    {
        if (plan is Filter filter)
        {
            Write($"Filter({filter.Predicate})", writer, ident);
            WriteOutputColumns(filter.OutputColumns, writer);
            WriteLine("", writer, ident);
            Explain(filter.Input, writer, ident + 1);
            return;
        }

        if (plan is Join join)
        {
            var joinCon = join.Condition != null ? " on " + join.Condition : "";
            Write($"Join({join.JoinType}{joinCon})", writer, ident);
            WriteOutputColumns(join.OutputColumns, writer);
            WriteLine("", writer, ident);
            Explain(join.Left, writer, ident + 1);
            WriteLine("", writer, ident);
            Explain(join.Right, writer, ident + 1);
            return;
        }

        if (plan is Aggregate aggregate)
        {
            Write($"Agg({Expressions(aggregate.Aggregates)}) group by ({Expressions(aggregate.GroupBy)})", writer, ident);
            WriteOutputColumns(aggregate.OutputColumns, writer);
            WriteLine("", writer, ident);
            Explain(aggregate.Input, writer, ident + 1);
            return;
        }

        if (plan is Projection project)
        {
            Write($"Project({Expressions(project.Expressions)})", writer, ident);
            WriteOutputColumns(project.OutputColumns, writer);
            WriteLine("", writer, ident);
            Explain(project.Input, writer, ident + 1);
            return;
        }

        if (plan is Distinct distinct)
        {
            Write("Distinct", writer, ident);
            WriteOutputColumns(distinct.OutputColumns, writer);
            WriteLine("", writer, ident);
            Explain(distinct.Input, writer, ident + 1);
            return;
        }

        if (plan is Sort sort)
        {
            Write($"Sort({Expressions(sort.OrderBy)})", writer, ident);
            WriteOutputColumns(sort.OutputColumns, writer);
            WriteLine("", writer, ident);
            Explain(sort.Input, writer, ident + 1);
            return;
        }

        if (plan is Scan scan)
        {
            var filterCon = scan.Filter != null ? " where " + scan.Filter : "";
            Write($"Scan({scan.Table}){filterCon}", writer, ident);
            WriteOutputColumns(scan.OutputColumns, writer);
            WriteLine("", writer, ident);
            return;
        }

        throw new NotImplementedException("Explain not implemented for this plan: {" + plan + "}");
    }

    private void Write(string s, StringWriter writer, int ident)
    {
        var output = string.Join(IdentString, Enumerable.Repeat(" ", ident));
        writer.Write(output + s);
    }

    private void WriteLine(string s, StringWriter writer, int ident)
    {
        var output = string.Join(IdentString, Enumerable.Repeat(" ", ident));
        writer.WriteLine(output + s);
    }

    private void WriteOutputColumns(IReadOnlyList<ColumnSchema> columns, StringWriter writer)
    {
        if (!IncludeOutputColumns)
        {
            return;
        }
        writer.Write(" -> " + string.Join(", ", columns.Select(c => c.SourceTableAlias + "." + c.Name)));
    }

    private string Expressions(IReadOnlyList<BaseExpression> expressions)
    {
        return string.Join(", ", expressions.Select(e => e.ToString()));
    }
}
