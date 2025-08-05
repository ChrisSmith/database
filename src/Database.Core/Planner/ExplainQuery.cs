using Database.Core.Catalog;
using Database.Core.Expressions;
using Database.Core.Operations;

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

    public string Explain(IOperation physicalPlan)
    {
        var writer = new StringWriter();
        Explain(physicalPlan, writer, 0);
        return writer.ToString();
    }

    public void Explain(IOperation physicalPlan, StringWriter writer, int ident)
    {
        if (physicalPlan is DistinctOperation d)
        {
            Write($"Distinct()", writer, ident);
            WriteOutputColumns(d.OutputColumns, writer);
            WriteLine("", writer, ident);
            Explain(d.Source, writer, ident + 1);
            return;
        }

        if (physicalPlan is FilterOperation f)
        {
            Write($"Filter({f.Expression})", writer, ident);
            WriteOutputColumns(f.OutputColumns, writer);
            WriteLine("", writer, ident);
            Explain(f.Source, writer, ident + 1);
            return;
        }

        if (physicalPlan is NestedLoopJoinOperator nlj)
        {
            Write($"NestedLoopJoin()", writer, ident);
            WriteOutputColumns(nlj.OutputColumns, writer);
            WriteLine("", writer, ident);
            Explain(nlj.LeftSource, writer, ident + 1);
            WriteLine("", writer, ident);
            Explain(nlj.RightSource, writer, ident + 1);
            return;
        }

        if (physicalPlan is ProjectionOperation p)
        {
            Write($"Project({Expressions(p.Expressions)})", writer, ident);
            WriteOutputColumns(p.OutputColumns, writer);
            WriteLine("", writer, ident);
            Explain(p.Source, writer, ident + 1);
            return;
        }

        if (physicalPlan is FileScanFusedFilter fsf)
        {
            Write($"FileScanFusedFilter({fsf.Expression}) on {fsf.Path}", writer, ident);
            WriteOutputColumns(fsf.OutputColumns, writer);
            return;
        }

        if (physicalPlan is HashJoinOperator hj)
        {
            Write($"HashJoin({Expressions(hj.ProbeKeys)}, {Expressions(hj.ScanKeys)})", writer, ident);
            WriteOutputColumns(hj.OutputColumns, writer);
            WriteLine("", writer, ident);
            Explain(hj.ScanSource, writer, ident + 1);
            WriteLine("", writer, ident);
            Explain(hj.ProbeSource, writer, ident + 1);
            return;
        }

        if (physicalPlan is FileScan fs)
        {
            Write($"FileScan({fs.Path})", writer, ident);
            WriteOutputColumns(fs.OutputColumns, writer);
            return;
        }

        if (physicalPlan is HashAggregate ha)
        {
            Write($"HashAggregate({Expressions(ha.OutputExpressions)})", writer, ident);
            WriteOutputColumns(ha.OutputColumns, writer);
            WriteLine("", writer, ident);
            Explain(ha.Source, writer, ident + 1);
            return;
        }

        if (physicalPlan is SortOperator so)
        {
            Write($"Sort({Expressions(so.OrderExpressions)})", writer, ident);
            WriteOutputColumns(so.OutputColumns, writer);
            WriteLine("", writer, ident);
            Explain(so.Source, writer, ident + 1);
            return;
        }

        if (physicalPlan is UngroupedAggregate uga)
        {
            Write($"UngroupedAggregate({Expressions(uga.Expressions)})", writer, ident);
            WriteOutputColumns(uga.OutputColumns, writer);
            WriteLine("", writer, ident);
            Explain(uga.Source, writer, ident + 1);
            return;
        }

        throw new NotImplementedException("Explain not implemented for this plan: {" + physicalPlan + "}");
    }
}
