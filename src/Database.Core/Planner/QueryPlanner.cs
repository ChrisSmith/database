using Database.Core.Catalog;
using Database.Core.Expressions;
using Database.Core.Functions;
using Database.Core.Operations;
using static Database.Core.TokenType;

namespace Database.Core.Planner;

public class QueryPlanner(Catalog.Catalog catalog)
{
    private FunctionRegistry _functions = new();

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

            if (select.Where != null)
            {
                var filterFunc = BindBinaryExpression(table, select.Where);
                source = new Filter(source, filterFunc);
            }

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

            result.Add(_functions.Bind(function.Name, function.Args, table));
        }

        return result;
    }

    private IFilterFunction BindBinaryExpression(TableSchema table, IExpression expression)
    {
        if (expression is BinaryExpression b)
        {
            if (b.Left is not ColumnExpression left)
            {
                throw new QueryPlanException($"left expression '{b.Left}' is not a column expression");
            }
            var (_, leftIndex) = FindColumnIndex(table, b.Left);

            if (b.Right is ColumnExpression right)
            {
                var (_, rightIndex) = FindColumnIndex(table, b.Right);

                return b.Operator switch
                {
                    LESS => new IntLessThanTwo(leftIndex, rightIndex),
                    _ => throw new ArgumentOutOfRangeException()
                };
            }

            if (b.Right is NumericLiteral num)
            {
                int rightValue = Convert.ChangeType(num.Literal, typeof(int)) as int? ?? throw new QueryPlanException($"right expression '{b.Right}' is not a numeric literal");
                return b.Operator switch
                {
                    LESS => new IntLessThanOne(leftIndex, rightValue),
                    _ => throw new ArgumentOutOfRangeException()
                };
            }

            throw new QueryPlanException($"right expression '{b.Right}' is not a column or numeric literal");
        }
        else
        {
            // TODO need to convert other expression type to binary expression
            throw new NotImplementedException($"unsupported expression type '{expression.GetType().Name}' for binary expression");
        }
    }

    private (List<string> names, List<int> indexes) Columns(SelectListStatement selectSelectList, TableSchema table)
    {
        var columnsNames = new List<string>();
        var columnsIndexes = new List<int>();

        for (var i = 0; i < selectSelectList.Expressions.Count; i++)
        {
            var (name, idx) = FindColumnIndex(table, selectSelectList.Expressions[i]);
            columnsNames.Add(name);
            columnsIndexes.Add(idx == -1 ? i : idx);
        }

        return (columnsNames, columnsIndexes);
    }

    private (string, int) FindColumnIndex(TableSchema table, IExpression exp)
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
            var (_, idx) = FindColumnIndex(table, alias.Expression);
            return (alias.Alias, idx);
        }
        if (exp is FunctionExpression fun)
        {
            return (fun.Name, -1); // function is bound to is position
        }
        throw new QueryPlanException($"Unsupported expression type '{exp.GetType().Name}'");
    }
}

public class QueryPlanException(string message) : Exception(message);
