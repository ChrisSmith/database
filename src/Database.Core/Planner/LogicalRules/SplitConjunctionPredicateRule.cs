using Database.Core.Expressions;
using Database.Core.Options;

namespace Database.Core.Planner.LogicalRules;

public class SplitConjunctionPredicateRule(ConfigOptions config)
{
    public bool CanRewrite(LogicalPlan plan)
    {
        if (!config.LogicalOptimization || !config.OptSplitPredicates)
        {
            return false;
        }
        return plan is Filter { Predicate: BinaryExpression { Operator: TokenType.AND } };
    }

    public LogicalPlan Rewrite(BindContext context, LogicalPlan root)
    {
        var filter = (Filter)root;
        var predicates = QueryRewriter.SplitConjunctions(filter.Predicate);

        var source = filter.Input;
        for (var i = 0; i < predicates.Count; i++)
        {
            source = new Filter(source, predicates[i]);
        }
        return source;
    }
}
