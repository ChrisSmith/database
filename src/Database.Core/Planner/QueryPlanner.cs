using Database.Core.Catalog;
using Database.Core.Expressions;
using Database.Core.Functions;
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

            IOperation source = new FileScan(table.Location);
            if (select.SelectList.Expressions
                .Select(e => e is AliasExpression alias ? alias.Expression : e)
                .Any(e => e is FunctionExpression)
               )
            {
                source = new Aggregate(source, BindExpressions(table, select.SelectList.Expressions));
            }

            var (names, indexes) = Columns(select.SelectList, table);
            var projection = new Projection(names, indexes, source);

            if (select.SelectList.Distinct)
            {
                var distinct = new Distinct(projection);
                return new QueryPlan(distinct);
            }

            return new QueryPlan(projection);
        }

        throw new QueryPlanException($"Unknown statement type '{statement.GetType().Name}'. Cannot create query plan.");
    }

    private List<AggregateValue> BindExpressions(TableSchema table, List<IExpression> expressions)
    {
        var result = new List<AggregateValue>();
        for (var i = 0; i < expressions.Count; i++)
        {
            var expression = expressions[i];
            if (expression is AliasExpression alias)
            {
                // TODO consider removing alias or making it a part of other expressions?
                expression = alias.Expression;
            }

            if (expression is not FunctionExpression function)
            {
                throw new QueryPlanException($"expression '{expression}' is not an aggregate function");
            }

            if (function.Name == "count")
            {
                var argument = function.Args.Single();
                if (argument is ColumnExpression columnExpr)
                {
                    var index = table.Columns.FindIndex(c => c.Name == columnExpr.Column);
                    var column = table.Columns[index];

                    switch (column.DataType)
                    {
                        case DataType.Int:
                            result.Add(new IntCount(index));
                            break;
                        case DataType.Double:
                            result.Add(new DoubleCount(index));
                            break;
                        case DataType.String:
                            result.Add(new StringCount(index));
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
                else if (argument is NumericLiteral or StarExpression)
                {
                    // TODO how do we want to handle constant values in expressions?
                    // result.Add(new DoubleCount(-1));
                    throw new NotImplementedException($"numeric literal or * not implemented for count yet");
                }
                else
                {
                    throw new QueryPlanException($"arguments to function '{function.Name}' are invalid got {argument}");
                }
            }
            else
            {
                throw new QueryPlanException($"function '{function.Name}' is invalid");
            }
        }

        return result;
    }

    private (List<string> names, List<int> indexes) Columns(SelectListStatement selectSelectList, TableSchema table)
    {
        var columnsNames = new List<string>();
        var columnsIndexes = new List<int>();

        for (var i = 0; i < selectSelectList.Expressions.Count; i++)
        {
            var (name, idx) = ColumnName(selectSelectList.Expressions[i]);
            columnsNames.Add(name);
            columnsIndexes.Add(idx == -1 ? i : idx);
        }

        return (columnsNames, columnsIndexes);

        (string, int) ColumnName(IExpression exp)
        {
            // TODO we need to actually handle * and alias
            if (exp is ColumnExpression column)
            {
                var colIdx = table.Columns.FindIndex(c => c.Name == column.Column);
                if (colIdx == -1)
                {
                    throw new QueryPlanException($"Column '{column.Column}' does not exist on table '{table.Name}'");
                }
                return (column.Column, colIdx);
            }
            if (exp is AliasExpression alias)
            {
                var (_, idx) = ColumnName(alias.Expression);
                return (alias.Alias, idx);
            }
            if (exp is FunctionExpression fun)
            {
                return (fun.Name, -1); // function is bound to is position
            }
            throw new QueryPlanException($"Unsupported expression type '{exp.GetType().Name}'");
        }
    }
}

public class QueryPlanException(string message) : Exception(message);
