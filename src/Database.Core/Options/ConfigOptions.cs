namespace Database.Core.Options;

public class ConfigOptions
{
    public bool LogicalOptimization { get; set; } = true;

    public bool CostBasedOptimization { get; set; } = true;
    public int MaxLogicalOptimizationSteps { get; set; } = 50;
    public bool ExplainIncludeOutputColumns { get; set; } = true;

    public bool OptProjectionPushDown { get; set; } = true;
    public bool OptJoin { get; set; } = true;
    public bool OptSplitPredicates { get; set; } = true;
    public bool OptPushDownFilter { get; set; } = true;
}
