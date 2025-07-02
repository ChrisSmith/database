using Database.Core.Catalog;
using Database.Core.Expressions;
using Database.Core.Operations;

namespace Database.Core.Planner;

public class QueryPlanner(Catalog.Catalog catalog)
{
    public QueryPlan CreatePlan(IStatement statement)
    {
        if (statement is SelectStatement select)
        {
            var from = select.From;
            var table = catalog.Tables.FirstOrDefault(t => t.Name == from.Table);
            if (table == null)
            {
                throw new QueryPlanException($"Table '{from.Table}' not found in catalog.");
            }

            var projection = new Projection(Columns(select.SelectList), new FileScan(table.Location));

            return new QueryPlan(projection);
        }

        throw new QueryPlanException($"Unknown statement type '{statement.GetType().Name}'. Cannot create query plan.");
    }

    private List<string> Columns(SelectListStatement selectSelectList)
    {
        string ColumnName(IExpression exp)
        {
            // TODO we need to actually handle * and alias
            if (exp is ColumnExpression column)
            {
                return column.Column;
            }
            if (exp is AliasExpression alias)
            {
                return ColumnName(alias.Expression);
            }
            throw new QueryPlanException($"Unsupported expression type '{exp.GetType().Name}'");
        }

        return selectSelectList.Expressions.Select(ColumnName).ToList();
    }
}

public class QueryPlanException(string message) : Exception(message);
