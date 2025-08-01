using Database.Core.Catalog;
using Database.Core.Expressions;

namespace Database.Core.Planner;

public static class QueryRewriter
{
    public static SelectStatement ExpandStarStatements(
        SelectStatement statement,
        Catalog.Catalog catalog)
    {
        if (statement.From.TableStatements.Any(t => t is not TableStatement))
        {
            throw new QueryPlanException("Non table table statements are not supported yet.");
        }

        var tablesStmts = statement
            .From
            .TableStatements.Where(t => t is TableStatement)
            .Cast<TableStatement>()
            .ToList();

        var expressions = statement.SelectList.Expressions;

        var tables = new List<TableSchema>(tablesStmts.Count);
        foreach (var tableStmt in tablesStmts)
        {
            // TODO aliases?
            var table = catalog.Tables.Single(t => t.Name == tableStmt.Table);
            tables.Add(table);
        }

        var results = new List<BaseExpression>(expressions.Count);

        foreach (var expr in expressions)
        {
            if (expr is not StarExpression star)
            {
                results.Add(expr);
                continue;
            }

            var filteredTables = tables;
            if (star.Table != null)
            {
                filteredTables = [filteredTables.Single(t => t.Name == star.Table)];
            }

            foreach (var table in filteredTables)
            {
                foreach (var c in table.Columns)
                {
                    results.Add(new ColumnExpression(c.Name)
                    {
                        Alias = c.Name,
                    });
                }
            }
        }

        return statement with
        {
            SelectList = statement.SelectList with { Expressions = results },
        };
    }
}
