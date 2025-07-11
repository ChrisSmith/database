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
                if (filterFunc is not BoolFunction predicate)
                {
                    // TODO cast values to "truthy"
                    throw new QueryPlanException($"Filter expression '{select.Where}' is not a boolean expression");
                }
                source = new Filter(source, predicate);
            }

            if (select.SelectList.Expressions
                .Select(e => e is AliasExpression alias ? alias.Expression : e)
                .Any(e => e is FunctionExpression)
               )
            {
                source = new Aggregate(source, BindExpressions(table, select.SelectList.Expressions));
            }

            // TODO pickup here, we need to evaluate the expression for each item in the select list
            // its not enough to take their index (ie Id + 1 as foo)
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

    private IFunction BindBinaryExpression(TableSchema table, IExpression expression)
    {
        if (expression is BinaryExpression b)
        {
            if (b.Left is not ColumnExpression left)
            {
                throw new QueryPlanException($"left expression '{b.Left}' is not a column expression");
            }
            var (_, leftIndex, leftType) = FindColumnIndex(table, b.Left);

            if (b.Right is ColumnExpression right)
            {
                var (_, rightIndex, rightType) = FindColumnIndex(table, b.Right);
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

                    (DataType.Int, GREATER) => new LessThanTwo<int>(rightIndex, leftIndex),
                    (DataType.Long, GREATER) => new LessThanTwo<long>(rightIndex, leftIndex),
                    (DataType.Float, GREATER) => new LessThanTwo<float>(rightIndex, leftIndex),
                    (DataType.Double, GREATER) => new LessThanTwo<double>(rightIndex, leftIndex),

                    (DataType.Int, GREATER_EQUAL) => new LessThanEqualTwo<int>(rightIndex, leftIndex),
                    (DataType.Long, GREATER_EQUAL) => new LessThanEqualTwo<long>(rightIndex, leftIndex),
                    (DataType.Float, GREATER_EQUAL) => new LessThanEqualTwo<float>(rightIndex, leftIndex),
                    (DataType.Double, GREATER_EQUAL) => new LessThanEqualTwo<double>(rightIndex, leftIndex),

                    (DataType.Int, EQUAL) => new EqualTwo<int>(leftIndex, rightIndex),
                    (DataType.Long, EQUAL) => new EqualTwo<long>(leftIndex, rightIndex),
                    (DataType.Float, EQUAL) => new EqualTwo<float>(leftIndex, rightIndex),
                    (DataType.Double, EQUAL) => new EqualTwo<double>(leftIndex, rightIndex),

                    (DataType.Int, BANG_EQUAL) => new NotEqualTwo<int>(leftIndex, rightIndex),
                    (DataType.Long, BANG_EQUAL) => new NotEqualTwo<long>(leftIndex, rightIndex),
                    (DataType.Float, BANG_EQUAL) => new NotEqualTwo<float>(leftIndex, rightIndex),
                    (DataType.Double, BANG_EQUAL) => new NotEqualTwo<double>(leftIndex, rightIndex),

                    _ => throw new QueryPlanException($"query plan doesn't support {b.Operator} {leftType.Value} yet")
                };
            }

            if (b.Right is NumericLiteral num)
            {
                var targetType = leftType!.Value.ClrTypeFromDataType();
                var rightValue = Convert.ChangeType(num.Literal, targetType) ?? throw new QueryPlanException($"right expression '{b.Right}' is not a numeric literal");
                var returnType = leftType.Value;

                return (returnType, b.Operator) switch
                {
                    (DataType.Int, LESS) => new LessThanOne<int>(leftIndex, (int)rightValue),
                    (DataType.Long, LESS) => new LessThanOne<long>(leftIndex, (long)rightValue),
                    (DataType.Float, LESS) => new LessThanOne<float>(leftIndex, (float)rightValue),
                    (DataType.Double, LESS) => new LessThanOne<double>(leftIndex, (double)rightValue),

                    (DataType.Int, LESS_EQUAL) => new LessThanEqualOne<int>(leftIndex, (int)rightValue),
                    (DataType.Long, LESS_EQUAL) => new LessThanEqualOne<long>(leftIndex, (long)rightValue),
                    (DataType.Float, LESS_EQUAL) => new LessThanEqualOne<float>(leftIndex, (float)rightValue),
                    (DataType.Double, LESS_EQUAL) => new LessThanEqualOne<double>(leftIndex, (double)rightValue),


                    (DataType.Int, EQUAL) => new EqualOne<int>(leftIndex, (int)rightValue),
                    (DataType.Long, EQUAL) => new EqualOne<long>(leftIndex, (long)rightValue),
                    (DataType.Float, EQUAL) => new EqualOne<float>(leftIndex, (float)rightValue),
                    (DataType.Double, EQUAL) => new EqualOne<double>(leftIndex, (double)rightValue),

                    (DataType.Int, BANG_EQUAL) => new NotEqualOne<int>(leftIndex, (int)rightValue),
                    (DataType.Long, BANG_EQUAL) => new NotEqualOne<long>(leftIndex, (long)rightValue),
                    (DataType.Float, BANG_EQUAL) => new NotEqualOne<float>(leftIndex, (float)rightValue),
                    (DataType.Double, BANG_EQUAL) => new NotEqualOne<double>(leftIndex, (double)rightValue),

                    (DataType.Int, PLUS) => new SumOne<int>(leftIndex, (int)rightValue, returnType),
                    (DataType.Long, PLUS) => new SumOne<long>(leftIndex, (long)rightValue, returnType),
                    (DataType.Float, PLUS) => new SumOne<float>(leftIndex, (float)rightValue, returnType),
                    (DataType.Double, PLUS) => new SumOne<double>(leftIndex, (double)rightValue, returnType),

                    (DataType.Int, MINUS) => new MinusOneRight<int>(leftIndex, (int)rightValue, returnType),
                    (DataType.Long, MINUS) => new MinusOneRight<long>(leftIndex, (long)rightValue, returnType),
                    (DataType.Float, MINUS) => new MinusOneRight<float>(leftIndex, (float)rightValue, returnType),
                    (DataType.Double, MINUS) => new MinusOneRight<double>(leftIndex, (double)rightValue, returnType),

                    (DataType.Int, STAR) => new MultiplyOne<int>(leftIndex, (int)rightValue, returnType),
                    (DataType.Long, STAR) => new MultiplyOne<long>(leftIndex, (long)rightValue, returnType),
                    (DataType.Float, STAR) => new MultiplyOne<float>(leftIndex, (float)rightValue, returnType),
                    (DataType.Double, STAR) => new MultiplyOne<double>(leftIndex, (double)rightValue, returnType),

                    _ => throw new QueryPlanException($"query plan doesn't support {b.Operator} {returnType} yet")
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
        if (exp is BinaryExpression b)
        {
            // probably want the literal text of the expression here to name the column
            return ("foo", -1, null); // function is bound to is position
        }
        throw new QueryPlanException($"Unsupported expression type '{exp.GetType().Name}'");
    }
}

public class QueryPlanException(string message) : Exception(message);
