using Database.Core.Expressions;
using Database.Core.Functions;
using Database.Core.Options;
using Database.Core.Planner.QueryGraph;

namespace Database.Core.Planner.LogicalRules;

public class CorrelatedSubQueryRule(ConfigOptions config, ExpressionBinder _binder)
{
    public bool CanRewrite(LogicalPlan plan)
    {
        if (!config.LogicalOptimization || !config.OptDecorrelateSubqueries)
        {
            return false;
        }
        if (plan is not Apply a)
        {
            return false;
        }
        if (a.Correlated.Count != 1)
        {
            return false;
        }

        var root = a.Correlated.Single();
        if (root is Projection p)
        {
            root = p.Input;
        }

        return root is Aggregate { Input: Filter or JoinSet };
    }

    public LogicalPlan Rewrite(BindContext context, LogicalPlan root)
    {
        var apply = (Apply)root;
        var rootPlan = apply.Input;

        var id = 0; // TODO handle multiple correlated subqueries
        var plan = apply.Correlated.Single();

        var input = plan.BindContext!.LateBoundSymbols.Single();
        var outerColumnName = input.Key.Item1;
        var outerTableAlias = input.Key.Item2;

        // TODO apply Improving Unnesting of Complex Queries by Thomas Neumann
        BaseExpression? projectionExpr = null;
        if (plan is Projection proj)
        {
            var expr = proj.Expressions.Single();
            if (expr is not ColumnExpression)
            {
                projectionExpr = expr;
            }
            plan = proj.Input;
        }

        if (plan is Aggregate { Input: Filter { Predicate: BinaryExpression { Operator: TokenType.EQUAL } p } f } agg)
        {
            // Drop the correlated filter and replace it with a group by on the input
            BaseExpression innerColumn = IsCorrelatedFunction(p.Left.BoundFunction)
                ? (ColumnExpression)p.Right
                : (ColumnExpression)p.Left;

            var source = f.Input;
            innerColumn = _binder.Bind(context, innerColumn, source.OutputSchema);
            var aggregates = _binder.Bind(context, [innerColumn, .. agg.Aggregates], source.OutputSchema);
            plan = new Aggregate(source, [innerColumn], aggregates, $"correlated{id}");
        }
        else if (plan is Aggregate { Input: JoinSet joinSet } agg2)
        {
            // Find the correlated column, add it to the aggregate
            var resFilters = new List<BaseExpression>();
            BaseExpression innerColumn = null!;

            foreach (var filt in joinSet.Filters)
            {
                if (filt is BinaryExpression binEx && binEx.AnyChildOrSelf(f => IsCorrelatedFunction(f.BoundFunction)))
                {
                    innerColumn = IsCorrelatedFunction(binEx.Left.BoundFunction)
                        ? (ColumnExpression)binEx.Right
                        : (ColumnExpression)binEx.Left;
                }
                else
                {
                    resFilters.Add(filt);
                }
            }

            if (innerColumn == null)
            {
                throw new QueryPlanException("Failed to find correlated column");
            }

            joinSet = joinSet with
            {
                Filters = resFilters,
            };

            innerColumn = _binder.Bind(context, innerColumn, joinSet.OutputSchema);
            plan = new Aggregate(joinSet, [innerColumn, .. agg2.GroupBy], [innerColumn, .. agg2.Aggregates], $"correlated{id}");

        }
        else
        {
            throw new QueryPlanException("Cannot rewrite correlated plan as group by on inputs");
        }

        var joinCol = plan.OutputSchema.First();
        var rightJoinCol = new ColumnExpression(joinCol.Name, $"correlated{id}");

        BinaryExpression? correlatedExpr = null;
        if (rootPlan is Filter { Predicate: BinaryExpression { } p2 } filter && IsCorrelatedFunction(p2.Right.BoundFunction))
        {
            // Remove the correlated filter and replace it with a join on the input
            rootPlan = filter.Input;
            correlatedExpr = (BinaryExpression)filter.Predicate;
        }
        else if (rootPlan is JoinSet joinSet2)
        {
            var keep = new List<Edge>();
            foreach (var edge in joinSet2.Edges)
            {
                if (edge is UnaryEdge { Expression: BinaryExpression { } b }
                    && (b.Left is SubQueryResultExpression || b.Right is SubQueryResultExpression))
                {
                    correlatedExpr = b;
                }
                else
                {
                    keep.Add(edge);
                }
            }

            rootPlan = joinSet2 with
            {
                Edges = keep,
            };
        }
        else
        {
            throw new QueryPlanException("Cannot rewrite correlated plan as group by on inputs");
        }

        if (correlatedExpr == null)
        {
            throw new QueryPlanException("Failed to find correlated expression");
        }

        var correlatedInnerCol = plan.OutputSchema.Last();

        var leftJoinCol = new ColumnExpression(outerColumnName, outerTableAlias);
        BaseExpression joinCond = new BinaryExpression(TokenType.EQUAL, "=", leftJoinCol, rightJoinCol);

        var schema = QueryPlanner.GetCombinedOutputSchema([rootPlan, plan]);
        joinCond = _binder.Bind(context, joinCond, schema);

        rootPlan = new Join(rootPlan, plan, JoinType.Inner, joinCond);



        BaseExpression rightExpr;
        if (projectionExpr != null)
        {
            rightExpr = projectionExpr;
            // projectionExpr = _binder.Bind(context, projectionExpr, rootPlan.OutputSchema);
            // rootPlan = new Projection(rootPlan,
            //     [projectionExpr],
            //     QueryPlanner.SchemaFromExpressions([projectionExpr], null),
            //     null
            //     );
        }
        else
        {
            rightExpr = new ColumnExpression(correlatedInnerCol.Name, correlatedInnerCol.SourceTableAlias);
        }

        correlatedExpr = correlatedExpr with
        {
            Right = rightExpr,
        };
        var correlatedFilter = _binder.Bind(context, correlatedExpr, rootPlan.OutputSchema);
        rootPlan = new Filter(rootPlan, correlatedFilter);
        // join cond rewrite should be t.CategoricalInt = max(CategoricalInt) and t.CategoricalString = q.CategoricalString

        return rootPlan;
    }

    private bool IsCorrelatedFunction(IFunction? function)
    {
        return function is UnboundCorrelatedSubQueryFunction or CorrelatedSubQueryFunction;
    }
}
