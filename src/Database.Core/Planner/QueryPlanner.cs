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
            var (_, leftIndex, rightType) = FindColumnIndex(table, b.Left);

            if (b.Right is ColumnExpression right)
            {
                var (_, rightIndex, leftType) = FindColumnIndex(table, b.Right);
                if (leftType != rightType || leftType == null || rightType == null)
                {
                    // TODO automatic type casts?
                    throw new QueryPlanException($"left and right expression types do not match. got {leftType} != {rightType}");
                }

                return (leftType.Value, b.Operator) switch
                {
                    (DataType.Int, LESS) => new LessThanTwo<int>(leftIndex, rightIndex),
                    (DataType.Long, LESS) => new LessThanTwo<long>(leftIndex, rightIndex),
                    (DataType.Float, LESS) => new LessThanTwo<float>(leftIndex, rightIndex),
                    (DataType.Double, LESS) => new LessThanTwo<double>(leftIndex, rightIndex),

                    (DataType.Int, LESS_EQUAL) => new LessThanEqualTwo<int>(leftIndex, rightIndex),
                    (DataType.Long, LESS_EQUAL) => new LessThanEqualTwo<long>(leftIndex, rightIndex),
                    (DataType.Float, LESS_EQUAL) => new LessThanEqualTwo<float>(leftIndex, rightIndex),
                    (DataType.Double, LESS_EQUAL) => new LessThanEqualTwo<double>(leftIndex, rightIndex),

                    (DataType.Int, EQUAL) => new EqualTwo<int>(leftIndex, rightIndex),
                    (DataType.Long, EQUAL) => new EqualTwo<long>(leftIndex, rightIndex),
                    (DataType.Float, EQUAL) => new EqualTwo<float>(leftIndex, rightIndex),
                    (DataType.Double, EQUAL) => new EqualTwo<double>(leftIndex, rightIndex),

                    (DataType.Int, BANG_EQUAL) => new NotEqualTwo<int>(leftIndex, rightIndex),
                    (DataType.Long, BANG_EQUAL) => new NotEqualTwo<long>(leftIndex, rightIndex),
                    (DataType.Float, BANG_EQUAL) => new NotEqualTwo<float>(leftIndex, rightIndex),
                    (DataType.Double, BANG_EQUAL) => new NotEqualTwo<double>(leftIndex, rightIndex),

                    _ => throw new ArgumentOutOfRangeException()
                };
            }

            if (b.Right is NumericLiteral num)
            {
                int rightValue = Convert.ChangeType(num.Literal, typeof(int)) as int? ?? throw new QueryPlanException($"right expression '{b.Right}' is not a numeric literal");
                return b.Operator switch
                {
                    LESS => new LessThanOne<int>(leftIndex, rightValue),
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
            var (name, idx, _) = FindColumnIndex(table, selectSelectList.Expressions[i]);
            columnsNames.Add(name);
            columnsIndexes.Add(idx == -1 ? i : idx);
        }

        return (columnsNames, columnsIndexes);
    }

    private (string, int, DataType?) FindColumnIndex(TableSchema table, IExpression exp)
    {
        // TODO we need to actually handle * and alias
        if (exp is ColumnExpression column)
        {
            var colIdx = table.Columns.FindIndex(c => c.Name == column.Column);
            if (colIdx == -1)
            {
                throw new QueryPlanException($"Column '{column.Column}' does not exist on table '{table.Name}'");
            }
            return (column.Column, colIdx, table.Columns[colIdx].DataType);
        }
        if (exp is AliasExpression alias)
        {
            var (_, idx, dataType) = FindColumnIndex(table, alias.Expression);
            return (alias.Alias, idx, dataType);
        }
        if (exp is FunctionExpression fun)
        {
            // Might need to eagerly bind functions so we have the datatypes
            return (fun.Name, -1, null); // function is bound to is position
        }
        throw new QueryPlanException($"Unsupported expression type '{exp.GetType().Name}'");
    }
}

public class QueryPlanException(string message) : Exception(message);
