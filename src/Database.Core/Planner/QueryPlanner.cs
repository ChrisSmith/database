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
        if (statement is not SelectStatement select)
        {
            throw new QueryPlanException(
                $"Unknown statement type '{statement.GetType().Name}'. Cannot create query plan.");
        }

        var from = select.From;
        var table = catalog.Tables.FirstOrDefault(t => t.Name == from.Table);
        if (table == null)
        {
            throw new QueryPlanException($"Table '{from.Table}' not found in catalog.");
        }

        IOperation source = new FileScan(table.Location);

        // If any projections require the computation of a new column, do it prior to the filters/aggregations
        // so that we can filter/aggregate on them too
        var binaryExpr = select.SelectList.Expressions
            .Where(e => e is BinaryExpression)
            .ToList();

        if (binaryExpr.Count != 0) // or FunctionExpression args any BinaryExpression
        {
            var columns = new List<ColumnSchema>(table.Columns);
            foreach (var expr in binaryExpr)
            {
                expr.BoundIndex = columns.Count;

                if (expr is BinaryExpression { Alias: "" } b)
                {
                    expr.Alias = b.Operator.ToString(); // TODO literal of the expression
                }

                BindExpression(expr, table);
                columns.Add(new ColumnSchema((ColumnId)(-1), expr.Alias, expr.BoundFunction!.ReturnType, expr.BoundFunction!.ReturnType.ClrTypeFromDataType()));
            }

            table = new TableSchema((TableId)(-1), "temp", columns, "memory");
            source = new ProjectionBinaryEval(table, source, binaryExpr);
        }

        if (select.Where != null)
        {
            BindExpression(select.Where, table);
            if (select.Where.BoundFunction is not BoolFunction predicate)
            {
                // TODO cast values to "truthy"
                throw new QueryPlanException($"Filter expression '{select.Where}' is not a boolean expression");
            }
            source = new Filter(source, select.Where);
        }

        if (select.SelectList.Expressions
            .Any(e => e is FunctionExpression)
           )
        {
            var aggregates = BindAggregateExpressions(table, select.SelectList.Expressions);
            source = new Aggregate(source, select.SelectList.Expressions);
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

    private List<IAggregateFunction> BindAggregateExpressions(TableSchema table, List<IExpression> expressions)
    {
        var result = new List<IAggregateFunction>();
        for (var i = 0; i < expressions.Count; i++)
        {
            var expr = expressions[i];
            BindExpression(expr, table);

            var expression = expressions[i];
            if (expression.BoundFunction is not IAggregateFunction function)
            {
                throw new QueryPlanException($"expression '{expression}' is not an aggregate function");
            }
            // TODO some in the grouping will be columns to hold constant
        }

        return result;
    }

    private void BindExpression(IExpression expression, TableSchema table)
    {
        expression.BoundFunction = FunctionForExpression(expression, table);
        expression.BoundDataType = expression.BoundFunction.ReturnType;
    }

    private IFunction FunctionForExpression(IExpression expression, TableSchema table)
    {
        if (expression.BoundFunction != null)
        {
            return expression.BoundFunction;
        }

        if (expression is IntegerLiteral numInt)
        {
            return new LiteralFunction(-1, numInt.Literal, DataType.Int);
        }

        if (expression is DoubleLiteral num)
        {
            return new LiteralFunction(-1, num.Literal, DataType.Double);
        }

        if (expression is StringLiteral str)
        {
            return new LiteralFunction(-1, str.Literal, DataType.String);
        }

        if (expression is BoolLiteral b)
        {
            return new LiteralFunction(-1, b.Literal, DataType.Bool);
        }

        if (expression is ColumnExpression column)
        {
            var (_, index, colType) = FindColumnIndex(table, column);
            return new SelectFunction(index, colType!.Value);
        }

        if (expression is BinaryExpression be)
        {
            BindExpression(be.Left, table);
            BindExpression(be.Right, table);

            var left = be.Left;
            var right = be.Right;

            if (left.BoundDataType == null || left.BoundDataType != right.BoundDataType)
            {
                // TODO automatic type casts?
                throw new QueryPlanException(
                    $"left and right expression types do not match. got {left.BoundDataType} != {right.BoundDataType}");
            }

            var args = new[] { left, right };
            return (be.Operator) switch
            {
                EQUAL => _functions.BindFunction("=", args, table),
                GREATER => _functions.BindFunction(">", args, table),
                GREATER_EQUAL => _functions.BindFunction(">=", args, table),
                LESS => _functions.BindFunction("<", args, table),
                LESS_EQUAL => _functions.BindFunction("<=", args, table),
                STAR => _functions.BindFunction("*", args, table),
                PLUS => _functions.BindFunction("+", args, table),
                MINUS => _functions.BindFunction("-", args, table),
                SLASH => _functions.BindFunction("/", args, table),
                PERCENT => _functions.BindFunction("%", args, table),
                _ => throw new QueryPlanException($"operator '{be.Operator}' not setup for binding yet"),
            };
        }

        if (expression is FunctionExpression fn)
        {
            foreach (var arg in fn.Args)
            {
                BindExpression(arg, table);
            }
            return _functions.BindFunction(fn.Name, fn.Args, table);
        }

        throw new NotImplementedException($"unsupported expression type '{expression.GetType().Name}' for expression binding");
    }

    private (List<string> names, List<int> indexes) Columns(SelectListStatement selectSelectList, TableSchema table)
    {
        var columnsNames = new List<string>();
        var columnsIndexes = new List<int>();

        for (var i = 0; i < selectSelectList.Expressions.Count; i++)
        {
            var expr = selectSelectList.Expressions[i];
            columnsNames.Add(expr.Alias);
            columnsIndexes.Add(expr.BoundIndex == -1 ? i : expr.BoundIndex);
        }

        return (columnsNames, columnsIndexes);
    }

    private (string, int, DataType?) FindColumnIndex(TableSchema table, IExpression exp)
    {
        if (exp.BoundIndex != -1)
        {
            return (exp.Alias, exp.BoundIndex, exp.BoundDataType);
        }

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
        if (exp is FunctionExpression fun)
        {
            // Might need to eagerly bind functions so we have the datatypes
            return (fun.Name, -1, null); // function is bound to is position
        }
        if (exp is BinaryExpression b)
        {
            // probably want the literal text of the expression here to name the column
            return (b.Alias, b.BoundIndex, b.BoundDataType); // function is bound to is position
        }
        throw new QueryPlanException($"Unsupported expression type '{exp.GetType().Name}'");
    }
}

public class QueryPlanException(string message) : Exception(message);
