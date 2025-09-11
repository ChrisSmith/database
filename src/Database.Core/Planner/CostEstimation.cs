using Database.Core.Expressions;
using Database.Core.Operations;

namespace Database.Core.Planner;

public class CostEstimation
{
    public const double EQUI_FILTER_SELECTIVITY = 0.2;
    public const double FILTER_SELECTIVITY = 0.9;


    public static double EstimateSelectivity(BaseExpression expr)
    {
        return expr switch
        {
            BinaryExpression { Operator: TokenType.EQUAL } => EQUI_FILTER_SELECTIVITY,
            _ => FILTER_SELECTIVITY,
        };
    }

    public static long EstimateExpressionCost(BaseExpression expr)
    {
        return expr is ColumnExpression or LiteralExpression ? 1 : 2;
    }

    public static long EstimateExpressionCost(IReadOnlyList<BaseExpression> exprs)
    {
        return exprs.Sum(EstimateExpressionCost);
    }
}
