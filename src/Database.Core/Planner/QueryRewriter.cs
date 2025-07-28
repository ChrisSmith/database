using Database.Core.Catalog;
using Database.Core.Expressions;

namespace Database.Core.Planner;

public static class QueryRewriter
{
    public static SelectStatement ExpandStarStatements(
        SelectStatement statement,
        Catalog.Catalog catalog)
    {

        var expressions = statement.SelectList.Expressions;
        var result = new List<BaseExpression>(expressions.Count);

        var from = statement.From;
        var table = catalog.Tables.FirstOrDefault(t => t.Name == from.Table);
        if (table == null)
        {
            throw new QueryPlanException($"Table '{from.Table}' not found in catalog.");
        }

        foreach (var expr in expressions)
        {
            result.AddRange(ExpandStarStatements(expr, table));
        }

        return statement with
        {
            SelectList = statement.SelectList with { Expressions = result },
        };
    }

    private static List<BaseExpression> ExpandStarStatements(
        BaseExpression expression,
        TableSchema table)
    {
        if (expression is StarExpression)
        {
            var result = new List<BaseExpression>(table.Columns.Count);
            foreach (var c in table.Columns)
            {
                result.Add(new ColumnExpression(c.Name)
                {
                    Alias = c.Name,
                });
            }
            return result;
        }

        return [expression];
    }

}
