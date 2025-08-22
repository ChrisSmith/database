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
            // TODO just skip star expansion for now
            //throw new QueryPlanException("Non table table statements are not supported yet.");
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
                        Table = table.Name,
                    });
                }
            }
        }

        return statement with
        {
            SelectList = statement.SelectList with { Expressions = results },
        };
    }

    public static SelectStatement DuplicateSelectExpressions(SelectStatement select)
    {
        if (select.Where == null)
        {
            return select;
        }

        // Allow aliases declared in the select expressions to be used in the where clause
        // SELECT Id + 1 as foo FROM table where foo = 11;

        var selectExpressions = select.SelectList.Expressions
            .Where(e => e is not ColumnExpression)
            .ToList();

        var updatedWhere = select.Where.Rewrite(expr =>
        {
            if (expr.Alias == "")
            {
                return expr;
            }

            if (expr is ColumnExpression { Table: null } ce)
            {
                var matching = selectExpressions.SingleOrDefault(e => e.Alias == ce.Alias);
                if (matching != null)
                {
                    // found a column being referenced with the same name as a complex select expression
                    // duplicate it in the where clause
                    return matching;
                }
            }
            return expr;
        });

        return select with { Where = updatedWhere };
    }

    public static (BaseExpression, List<SubQueryPlan> result) ExtractSubqueries(BaseExpression expression, int subQueryId = 0)
    {
        var subQueryPlans = new List<SubQueryPlan>();

        var updatedExpression = expression.Rewrite(expr =>
        {
            if (expr is SubQueryExpression subQuery)
            {
                var subResult = new SubQueryResultExpression(++subQueryId)
                {
                    Alias = $"$subquery_{subQueryId}$",
                };
                subQueryPlans.Add(new SubQuerySelectPlan(subQuery.Select, subResult));

                return subResult;
            }

            if (expr is ExpressionList exprList)
            {
                // The expression list might be correlated with the outer
                // query, transform it into an equivalent select and execute that
                var subResult = new SubQueryResultExpression(++subQueryId)
                {
                    Alias = $"$subquery_{subQueryId}$",
                };

                if (!exprList.Statements.All(s => s is LiteralExpression))
                {
                    throw new QueryPlanException("IN clause expression list currently only support literals.");
                }

                subQueryPlans.Add(new SubQueryInPlan(exprList, subResult));

                return subResult;

                // TODO Use UNION ALL here to join
                // the expressions into the query query plan
                // https://www.sqlite.org/lang_select.html
                /*
                var select = new SelectStatement(
                    new SelectListStatement(false, expressions),
                    null, null, null, null, null, null);
                subQueryPlans.Add(new SubQueryPlan(select, subResult));
                */
            }

            return expr;
        });

        return (updatedExpression, subQueryPlans);
    }
}

// TODO dependencies? Arguments?
public record SubQueryPlan(SubQueryResultExpression Expression);

public record SubQuerySelectPlan(SelectStatement Select, SubQueryResultExpression Expression) : SubQueryPlan(Expression);
public record SubQueryInPlan(ExpressionList ExpressionList, SubQueryResultExpression Expression) : SubQueryPlan(Expression);

