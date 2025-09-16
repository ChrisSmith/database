using System.Diagnostics.CodeAnalysis;
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
                var subResult = new SubQueryResultExpression(++subQueryId, Correlated: false)
                {
                    Alias = $"$subquery_{subQueryId}$",
                };
                subQueryPlans.Add(new SubQuerySelectPlan(subQuery.Select, subResult, subQuery.ExistsOnly));

                return subResult;
            }

            if (expr is ExpressionList exprList)
            {
                // The expression list might be correlated with the outer
                // query, transform it into an equivalent select and execute that
                var subResult = new SubQueryResultExpression(++subQueryId, Correlated: false)
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

    // TODO this needs test cases
    // Right now its going to make transformations that aren't valid
    public static BaseExpression RewriteDisjunction(BaseExpression expression)
    {
        // Example from query_07
        // (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')

        // Distribute the OR into two ANDs
        // (n1.name = 'FRANCE' or n1.name = 'GERMANY') and (n2.name = 'GERMANY' or n2.name = 'FRANCE') and n1.name <> n2.name

        // to
        // n1.n_name in ('FRANCE', 'GERMANY') and n2.n_name in ('GERMANY', 'FRANCE') and n1.n_name <> n2.n_name
        if (expression is not BinaryExpression { Operator: TokenType.OR } b)
        {
            return expression;
        }

        var statments = SplitDisjunctions(expression);
        if (statments.Count >= 3)
        {
            return HoistDisjunction(statments);
        }

        var leftConjuncts = SplitConjunctions(b.Left);
        var rightConjuncts = SplitConjunctions(b.Right);

        // TODO would be better to resolve symbols first

        var columnToLiteral = new Dictionary<string, List<LiteralExpression>>();

        foreach (var conjunct in leftConjuncts.Concat(rightConjuncts))
        {
            if (!IsEqualityOnLiteral(conjunct, out var column, out var literal))
            {
                return expression;
            }

            var columnName = column.Table != null ? $"{column.Table}.{column.Column}" : column.Column;
            if (!columnToLiteral.TryGetValue(columnName, out var literals))
            {
                literals = new List<LiteralExpression>();
                columnToLiteral.Add(columnName, literals);
            }
            literals.Add(literal);
        }

        var columns = columnToLiteral.Keys.ToList();
        if (columns.Count != 2)
        {
            return expression;
        }

        var inequality = new BinaryExpression(TokenType.BANG_EQUAL, "!=",
            ColumnExpression.FromString(columns[0]),
            ColumnExpression.FromString(columns[1])
            );

        var expressions = new List<BaseExpression>();
        foreach (var (column, literals) in columnToLiteral)
        {
            var columnExpr = ColumnExpression.FromString(column);
            BaseExpression? left = null;
            foreach (var literal in literals)
            {
                var comp = new BinaryExpression(TokenType.EQUAL, "=", columnExpr, literal);
                if (left != null)
                {
                    left = new BinaryExpression(TokenType.OR, "or", left, comp);
                }
                else
                {
                    left = comp;
                }
            }

            expressions.Add(left);
        }

        var newConj = new BinaryExpression(TokenType.AND, "and", expressions[0], expressions[1]);
        newConj = new BinaryExpression(TokenType.AND, "and", newConj, inequality);

        return newConj;


        bool IsEqualityOnLiteral(
            BaseExpression e,
            [NotNullWhen(true)] out ColumnExpression? column,
            [NotNullWhen(true)] out LiteralExpression? lit)
        {
            column = null;
            lit = null;

            if (e is BinaryExpression { Operator: TokenType.EQUAL } b)
            {
                if (b is { Left: ColumnExpression c1, Right: LiteralExpression l1 })
                {
                    column = c1;
                    lit = l1;
                    return true;
                }

                if (b is { Left: LiteralExpression l2, Right: ColumnExpression c2 })
                {
                    column = c2;
                    lit = l2;
                    return true;
                }
            }
            return false;
        }
    }

    private static BaseExpression HoistDisjunction(List<BaseExpression> statements)
    {
        if (statements.Count == 1)
        {
            return statements[0];
        }

        var conjuncts = new List<List<BaseExpression>>(statements.Count);

        foreach (var stmt in statements)
        {
            conjuncts.Add(SplitConjunctions(stmt));
        }

        var hoisted = new HashSet<BaseExpression>();

        // Find any expressions that exist in all lists
        for (var i = 0; i < conjuncts.Count; i++)
        {
            var list = conjuncts[i];
            foreach (var expr in list)
            {
                var existsInAll = conjuncts.All(l => l.Contains(expr));
                if (existsInAll)
                {
                    hoisted.Add(expr);
                }
            }
        }

        foreach (var conjunct in conjuncts)
        {
            foreach (var expr in hoisted)
            {
                conjunct.Remove(expr);
            }
        }

        var left = JoinJunction(hoisted.ToList(), TokenType.AND, "and");
        var rejoined = new List<BaseExpression>();
        foreach (var conjunct in conjuncts)
        {
            rejoined.Add(JoinJunction(conjunct, TokenType.AND, "and"));
        }
        var right = JoinJunction(rejoined, TokenType.OR, "or");

        var result = new BinaryExpression(TokenType.AND, "and", left, right);
        return result;
    }

    private static BaseExpression JoinJunction(IReadOnlyList<BaseExpression> expressions, TokenType op, string opLiteral)
    {
        if (expressions.Count < 2)
        {
            return expressions.Single();
        }

        var result = expressions[0];
        for (var i = 1; i < expressions.Count; i++)
        {
            result = new BinaryExpression(op, opLiteral, result, expressions[i]);
        }
        return result;
    }

    public static List<BaseExpression> SplitConjunctions(BaseExpression? expr)
    {
        return SplitBinaryExpression(expr, TokenType.AND);
    }

    public static List<BaseExpression> SplitDisjunctions(BaseExpression? expr)
    {
        return SplitBinaryExpression(expr, TokenType.OR);
    }

    private static List<BaseExpression> SplitBinaryExpression(BaseExpression? expr, TokenType op)
    {
        if (expr == null)
        {
            return [];
        }

        var result = new List<BaseExpression>();
        var queue = new Queue<BaseExpression>();
        queue.Enqueue(expr);
        while (queue.Count > 0)
        {
            var current = queue.Dequeue();
            if (current is BinaryExpression binExpr && binExpr.Operator == op)
            {
                queue.Enqueue(binExpr.Left);
                queue.Enqueue(binExpr.Right);
            }
            else
            {
                result.Add(current);
            }
        }
        return result;
    }

    public static List<BaseExpression> SplitRewriteSplitConjunctions(BaseExpression? expression)
    {
        if (expression == null)
        {
            return [];
        }
        var results = new List<BaseExpression>();
        foreach (var expr in SplitConjunctions(expression))
        {
            var opt = RewriteDisjunction(expr);
            results.AddRange(SplitConjunctions(opt));
        }
        return results;
    }
}

// TODO dependencies? Arguments?
public record SubQueryPlan(SubQueryResultExpression Expression);

public record SubQuerySelectPlan(SelectStatement Select, SubQueryResultExpression Expression, bool ExistsOnly) : SubQueryPlan(Expression);
public record SubQueryInPlan(ExpressionList ExpressionList, SubQueryResultExpression Expression) : SubQueryPlan(Expression);

