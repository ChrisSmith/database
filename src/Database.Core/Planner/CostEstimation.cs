using Database.Core.Expressions;
using Database.Core.Operations;

namespace Database.Core.Planner;

public class CostEstimation
{
    public static long EstimateExpressionCost(BaseExpression expr)
    {
        return expr is ColumnExpression or LiteralExpression ? 1 : 2;
    }

    public static long EstimateExpressionCost(IReadOnlyList<BaseExpression> exprs)
    {
        return exprs.Sum(EstimateExpressionCost);
    }
}
