using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Contracts;
using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;
using Database.Core.Operations;
using static Database.Core.TokenType;

namespace Database.Core.Planner;

public class ExpressionBinder(ParquetPool bufferPool, FunctionRegistry functions)
{
    [Pure]
    public IReadOnlyList<OrderingExpression> Bind(
        BindContext context,
        IReadOnlyList<OrderingExpression> expressions,
        IReadOnlyList<ColumnSchema> columns,
        bool ignoreMissingColumns = false
    )
    {
        var result = new List<OrderingExpression>(expressions.Count);
        foreach (var expr in expressions)
        {
            result.Add((OrderingExpression)Bind(context, expr, columns, ignoreMissingColumns));
        }
        return result;
    }

    [Pure]
    public IReadOnlyList<BaseExpression> Bind(
        BindContext context,
        IReadOnlyList<BaseExpression> expressions,
        IReadOnlyList<ColumnSchema> columns,
        bool ignoreMissingColumns = false
    )
    {
        var result = new List<BaseExpression>(expressions.Count);
        foreach (var expr in expressions)
        {
            result.Add(Bind(context, expr, columns, ignoreMissingColumns));
        }
        return result;
    }

    [Pure]
    public BaseExpression Bind(
        BindContext context,
        BaseExpression expression,
        IReadOnlyList<ColumnSchema> columns,
        bool ignoreMissingColumns = false
        )
    {
        IFunction? function = expression switch
        {
            IntegerLiteral numInt => new LiteralFunction(numInt.Literal, DataType.Int),
            DecimalLiteral num => new LiteralFunction(num.Literal, DataType.Decimal),
            StringLiteral str => new LiteralFunction(str.Literal, DataType.String),
            BoolLiteral b => new LiteralFunction(b.Literal, DataType.Bool),
            DateLiteral d => new LiteralFunction(d.Literal, DataType.Date),
            DateTimeLiteral dt => new LiteralFunction(dt.Literal, DataType.DateTime),
            IntervalLiteral il => new LiteralFunction(il.Literal, DataType.Interval),
            StarExpression => new LiteralFunction(1, DataType.Int), // TODO this is a bit of a hack
            _ => null,
        };

        if (function == null)
        {
            if (expression is ColumnExpression column)
            {
                var (_, columnRef, colType, isCorrelatedLookup) = FindColumnIndex(context, columns, column, ignoreMissingColumns);

                if (colType == DataType.Unknown)
                {
                    // Late bound column for nested subquery
                    function = new UnboundCorrelatedSubQueryFunction();
                }
                else
                {
                    if (isCorrelatedLookup)
                    {
                        var (subqueryId, tableRef) = context.CorrelatedSubQueryInputs.Single();
                        var table = bufferPool.GetMemoryTable(tableRef.TableId);
                        function = new SelectSubQueryFunction(columnRef, colType!.Value, table, bufferPool);
                    }
                    else
                    {
                        function = new SelectFunction(columnRef, colType!.Value, bufferPool);
                    }
                }
            }
            else if (expression is UnaryExpression unary)
            {
                var expr = Bind(context, unary.Expression, columns, ignoreMissingColumns);
                var args = new[] { expr };
                expression = unary with
                {
                    Expression = expr
                };
                function = (unary.Operator) switch
                {
                    NOT => functions.BindFunction("not", args),
                    _ => throw new QueryPlanException($"unary operator '{unary.Operator}' not setup for binding yet"),
                };
            }
            else if (expression is CastExpression cast)
            {
                var expr = Bind(context, cast.Expression, columns, ignoreMissingColumns);
                var args = new[] { expr };
                expression = cast with
                {
                    Expression = expr
                };
                function = functions.BindFunction(CastFnForType(cast.BoundDataType!.Value), args);
            }
            else if (expression is BinaryExpression be)
            {
                var left = Bind(context, be.Left, columns, ignoreMissingColumns);
                var right = Bind(context, be.Right, columns, ignoreMissingColumns);

                (left, right) = MakeCompatibleTypes(context, left, right, columns, ignoreMissingColumns);

                expression = be with
                {
                    Left = left,
                    Right = right,
                };

                var args = new[] { left, right };
                function = (be.Operator) switch
                {
                    BANG_EQUAL => functions.BindFunction("!=", args),
                    EQUAL => functions.BindFunction("=", args),
                    GREATER => functions.BindFunction(">", args),
                    GREATER_EQUAL => functions.BindFunction(">=", args),
                    LESS => functions.BindFunction("<", args),
                    LESS_EQUAL => functions.BindFunction("<=", args),
                    STAR => functions.BindFunction("*", args),
                    PLUS => functions.BindFunction("+", args),
                    MINUS => functions.BindFunction("-", args),
                    SLASH => functions.BindFunction("/", args),
                    PERCENT => functions.BindFunction("%", args),
                    AND => functions.BindFunction("and", args),
                    OR => functions.BindFunction("or", args),
                    LIKE => functions.BindFunction("like", args),
                    IN => new ExpressionListFn(DataType.Bool),
                    _ => throw new QueryPlanException($"operator '{be.Operator}' not setup for binding yet"),
                };
            }
            else if (expression is BetweenExpression bt)
            {
                var value = Bind(context, bt.Value, columns, ignoreMissingColumns);
                var lower = Bind(context, bt.Lower, columns, ignoreMissingColumns);
                var upper = Bind(context, bt.Upper, columns, ignoreMissingColumns);

                var compatType = FindCompatibleType([value, lower, upper]);
                value = DoCast(value, compatType, context, columns, ignoreMissingColumns);
                lower = DoCast(lower, compatType, context, columns, ignoreMissingColumns);
                upper = DoCast(upper, compatType, context, columns, ignoreMissingColumns);

                expression = bt with
                {
                    Value = value,
                    Lower = lower,
                    Upper = upper,
                };
                var name = bt.Negate ? "not_between" : "between";
                function = functions.BindFunction(name, [value, lower, upper]);
            }
            else if (expression is FunctionExpression fn)
            {
                var boundArgs = new BaseExpression[fn.Args.Length];
                for (var i = 0; i < fn.Args.Length; i++)
                {
                    boundArgs[i] = Bind(context, fn.Args[i], columns, ignoreMissingColumns);
                }

                expression = fn with
                {
                    Args = boundArgs,
                };
                function = functions.BindFunction(fn.Name, boundArgs);
            }
            else if (expression is OrderingExpression order)
            {
                var inner = Bind(context, order.Expression, columns, ignoreMissingColumns);
                function = inner.BoundFunction; // Is this ok?
                expression = order with
                {
                    Expression = inner,
                };
            }
            else if (expression is SubQueryResultExpression subQueryRes)
            {
                if (!context.BoundSymbols.TryGetValue(subQueryRes.Alias, out var symbol))
                {
                    throw new QueryPlanException($"subquery result '{subQueryRes.Alias}' was not found in the context");
                }
                symbol.RefCount++;

                if (!subQueryRes.Correlated)
                {
                    var outputTable = bufferPool.GetMemoryTable(subQueryRes.BoundMemoryTable.TableId);
                    function = new SelectSubQueryFunction(symbol.ColumnRef, symbol.DataType, outputTable, bufferPool);
                }
                else
                {
                    // Will be bound later by the physical planner
                    var subQueryOp = (subQueryRes.BoundFunction as CorrelatedSubQueryFunction)?.SubQuery;
                    var inputTable = subQueryRes.BoundInputMemoryTable != default
                        ? bufferPool.GetMemoryTable(subQueryRes.BoundInputMemoryTable.TableId)
                        : null!;

                    if (subQueryOp == null && context.CorrelatedSubQueryOps.Count != 0)
                    {
                        subQueryOp = context.CorrelatedSubQueryOps.Single(); // TODO
                    }

                    function = new CorrelatedSubQueryFunction(
                        [],
                        [],
                        symbol.DataType,
                        inputTable,
                        bufferPool,
                        subQueryOp!
                    );
                }
            }
            else if (expression is CaseExpression caseExpr)
            {
                var boundConditions = new List<BaseExpression>(caseExpr.Conditions.Count);
                foreach (var condition in caseExpr.Conditions)
                {
                    boundConditions.Add(Bind(context, condition, columns, ignoreMissingColumns));
                }
                var boundResults = new List<BaseExpression>(caseExpr.Results.Count);
                foreach (var result in caseExpr.Results)
                {
                    boundResults.Add(Bind(context, result, columns, ignoreMissingColumns));
                }

                BaseExpression? boundDefault = null;
                DataType compatType;
                if (caseExpr.Default != null)
                {
                    boundDefault = Bind(context, caseExpr.Default, columns, ignoreMissingColumns);

                    compatType = FindCompatibleType([.. boundResults, boundDefault]);
                    boundDefault = DoCast(boundDefault, compatType, context, columns, ignoreMissingColumns);
                }
                else
                {
                    compatType = FindCompatibleType(boundResults);
                }

                boundResults = boundResults.Select(e => DoCast(e, compatType, context, columns, ignoreMissingColumns)).ToList();


                function = new CaseWhen(boundResults[0].BoundDataType!.Value);
                expression = caseExpr with
                {
                    Conditions = boundConditions,
                    Results = boundResults,
                    Default = boundDefault,
                };
            }
            else if (expression is ExpressionList list)
            {
                var bound = new List<BaseExpression>();
                foreach (var expr in list.Statements)
                {
                    bound.Add(Bind(context, expr, columns, ignoreMissingColumns));
                }
                var compatType = FindCompatibleType(bound);
                bound = bound.Select(e => DoCast(e, compatType, context, columns, ignoreMissingColumns)).ToList();

                function = new ExpressionListFn(compatType);
                expression = list with
                {
                    Statements = bound,
                };
            }
            else
            {
                throw new NotImplementedException($"unsupported expression type '{expression.GetType().Name}' for expression binding");
            }
        }

        var alias = expression.Alias;
        if (alias == "")
        {
            alias = expression.ToString();
        }
        return expression with
        {
            BoundFunction = function,
            BoundDataType = function.ReturnType,
            Alias = alias,
        };
    }

    private DataType FindCompatibleType(IReadOnlyList<BaseExpression> expressions)
    {
        if (expressions.Count == 0)
        {
            throw new QueryPlanException("no expressions passed to find compatible types");
        }

        foreach (var expr in expressions)
        {
            if (expr.BoundDataType == null)
            {
                throw new QueryPlanException($"expression isn't bound to data type. {expr}");
            }
        }

        var allTypes = expressions.Select(e => e.BoundDataType!.Value).Distinct().ToList();
        if (allTypes.Count == 1)
        {
            return allTypes[0];
        }

        if (allTypes.Count == 2 && allTypes.Contains(DataType.Unknown))
        {
            return allTypes.Single(a => a != DataType.Unknown);
        }

        var integers = new[] { DataType.Int, DataType.Long };

        if (allTypes.All(t => integers.Contains(t)))
        {
            return DataType.Long;
        }

        if (allTypes.All(t => t == DataType.Decimal || integers.Contains(t)))
        {
            return DataType.Decimal;
        }

        var floating = new[] { DataType.Float, DataType.Double };
        if (allTypes.All(t => floating.Contains(t)))
        {
            return DataType.Double;
        }

        if (expressions.Any(ExpressionIsNonLiteralDecimal))
        {
            return DataType.Decimal;
        }

        foreach (var floatType in floating)
        {
            if (allTypes.All(t => t == DataType.Decimal || t == floatType))
            {
                return floatType;
            }
        }

        var dates = new[] { DataType.Date, DataType.DateTime };
        if (allTypes.All(t => dates.Contains(t)))
        {
            return DataType.DateTime;
        }

        var allTypesStr = string.Join(", ", allTypes.Select(t => t.ToString()));
        throw new QueryPlanException($"unable to automatically convert types '{allTypesStr}' to a compatible type.");

        bool ExpressionIsNonLiteralDecimal(BaseExpression expr)
        {
            return expr is not DecimalLiteral && expr.BoundDataType!.Value == DataType.Decimal;
        }
    }

    private (BaseExpression left, BaseExpression right) MakeCompatibleTypes(
        BindContext context,
        BaseExpression left,
        BaseExpression right,
        IReadOnlyList<ColumnSchema> columns,
        bool ignoreMissingColumns
        )
    {
        var compatType = FindCompatibleType([left, right]);
        return (
            DoCast(left, compatType, context, columns, ignoreMissingColumns),
            DoCast(right, compatType, context, columns, ignoreMissingColumns)
        );
    }

    private BaseExpression DoCast(
        BaseExpression expr,
        DataType targetType,
        BindContext context,
        IReadOnlyList<ColumnSchema> columns,
        bool ignoreMissingColumns)
    {
        if (expr.BoundDataType!.Value == targetType)
        {
            return expr;
        }

        if (expr.BoundDataType!.Value == DataType.Unknown)
        {
            // Assume the cast will exist once we figure out the type
            return expr with
            {
                BoundDataType = targetType,
            };
        }

        return Bind(context, new CastExpression(expr, targetType)
        {
            Alias = expr.Alias,
        }, columns, ignoreMissingColumns);
    }

    private string CastFnForType(DataType targetType)
    {
        return targetType switch
        {
            DataType.Int => "cast_int",
            DataType.Long => "cast_long",
            DataType.Float => "cast_float",
            DataType.Double => "cast_double",
            DataType.Decimal => "cast_decimal",
            DataType.DateTime => "cast_datetime",
            _ => throw new QueryPlanException($"unsupported cast type '{targetType}'"),
        };
    }

    private (string, ColumnRef, DataType?, bool) FindColumnIndex(
        BindContext context,
        IReadOnlyList<ColumnSchema> columns,
        ColumnExpression column,
        bool ignoreMissingColumns
        )
    {
        ColumnSchema? col = null;
        // TODO I'm not super happy with how aliases are handled
        var matchingColumns = columns.Where(c =>
        {
            return c.Name == column.Column && (column.Table == null || column.Table == c.SourceTableAlias || column.Table == c.SourceTableName);
        }).ToList();

        context.ReferenceSymbol(column.Column, column.Table, out var symbol);

        if (matchingColumns.Count == 1)
        {
            col = matchingColumns[0];
        }

        // if (col != null && col.SourceTableAlias == "" && col.SourceTableName == "")
        // {
        //     throw new QueryPlanException($"Column '{column.Column}' doesn't have a source table name or alias defined");
        // }

        if (col == null)
        {
            if (ignoreMissingColumns)
            {
                return (column.Column, default, DataType.Unknown, false);
            }

            if (matchingColumns.Count > 1)
            {
                var duplicates = string.Join(", ", matchingColumns.Select(c => c.Name));
                throw new QueryPlanException($"Unable to disambiguate duplicate Column '{column.Column}' from list of available columns {duplicates}");
            }

            if (context.SupportsLateBinding)
            {
                context.ReferenceLateBoundSymbol(column.Column, column.Table);
                return (column.Column, default, DataType.Unknown, false);
            }

            if (symbol != null && context.CorrelatedSubQueryInputs.Count != 0)
            {
                // correlated subquery input
                var (subquery, corrInput) = context.CorrelatedSubQueryInputs.Single(); // TODO
                var inputTable = bufferPool.GetMemoryTable(corrInput.TableId);
                col = inputTable.Schema.FirstOrDefault(c => c.Name == column.Column);

                var subQueryColumns = string.Join(", ", inputTable.Schema.Select(c => $"{c.SourceTableAlias}.{c.Name}"));
                if (col == null)
                {
                    throw new QueryPlanException($"Column '{column.Column}' was not found in the list of available columns from subquery {subQueryColumns}");
                }
                return (col.Name, col.ColumnRef, col.DataType, true);
            }


            var columnNames = string.Join(", ", columns.Select(c => $"{c.SourceTableAlias}.{c.Name}"));
            throw new QueryPlanException($"Column '{column.Table}.{column.Column}' was not found in list of available columns {columnNames}");
        }

        return (column.Column, col.ColumnRef, col.DataType, false);
    }

}

public class BindContext
{
    // Do we want to separate known symbols from aliases? known can be duped, alias can't?
    public Dictionary<string, BindSymbol> BoundSymbols { get; } = new();

    public bool SupportsLateBinding { get; set; }

    public HashSet<Tuple<string, string?>> LateBoundSymbols { get; } = new();

    public List<IOperation> CorrelatedSubQueryOps { get; } = new();

    // subqueryId to memory ref
    public List<Tuple<int, MemoryStorage>> CorrelatedSubQueryInputs { get; } = new();

    public void AddSymbols(TableSchema table, string? tableStmtAlias)
    {
        AddSymbols(table.Name, table.Columns, tableStmtAlias);
    }

    public void AddSymbols(BindContext other)
    {
        foreach (var (key, symbol) in other.BoundSymbols)
        {
            BoundSymbols.TryAdd(key, symbol);
        }
    }

    public void AddSymbols(string tableName, IReadOnlyList<ColumnSchema> columns, string? tableStmtAlias)
    {
        foreach (var column in columns)
        {
            var symbol = new BindSymbol(column.Name, tableName, column.DataType, column.ColumnRef, 0);
            TryAddSymbol(column.Name, symbol);
            TryAddSymbol($"{tableName}.{column.Name}", symbol);
            if (tableStmtAlias != null)
            {
                TryAddSymbol($"{tableStmtAlias}.{column.Name}", symbol);
            }
        }
    }

    public bool TryAddSymbol(string ident, BindSymbol symbol)
    {
        return BoundSymbols.TryAdd(ident, symbol);
    }

    public void AddSymbol(SubQueryResultExpression expression)
    {
        if (expression.BoundDataType == null || expression.BoundOutputColumn == default || expression.Alias == "")
        {
            throw new QueryPlanException($"subquery result must be bound prior to adding symbol");
        }
        var alias = expression.Alias;
        BoundSymbols[alias] = new BindSymbol(alias, $"", expression.BoundDataType.Value, expression.BoundOutputColumn, 0);
    }

    public bool ReferenceSymbol(
        string columnName,
        string? tableOrAlias,
        [NotNullWhen(true)] out BindSymbol? symbol)
    {

        if (tableOrAlias == null)
        {
            if (BoundSymbols.TryGetValue(columnName, out symbol))
            {
                symbol.RefCount++;
                return true;
            }
            return false;
        }

        if (BoundSymbols.TryGetValue($"{tableOrAlias}.{columnName}", out symbol))
        {
            symbol.RefCount++;
            return true;
        }
        else
        {
            // throw new QueryPlanException($"Column '{columnName}' was not found in the list of available columns");
        }
        return false;
    }

    public void ReferenceLateBoundSymbol(string columnName, string? tableOrAlias)
    {
        LateBoundSymbols.Add(new Tuple<string, string?>(columnName, tableOrAlias));
    }
}

[DebuggerDisplay("{TableName}.{Name} {DataType} {ColumnRef} {RefCount}")]
public class BindSymbol(string name, string tableName, DataType dataType, ColumnRef columnRef, int refCount)
{
    public string Name { get; } = name;

    public string TableName { get; } = tableName;

    public DataType DataType { get; } = dataType;
    public ColumnRef ColumnRef { get; } = columnRef;
    public int RefCount { get; set; } = refCount;
}
