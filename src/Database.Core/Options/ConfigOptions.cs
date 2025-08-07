namespace Database.Core.Options;

public class ConfigOptions
{
    public bool LogicalOptimization { get; set; } = true;

    public bool CostBasedOptimization { get; set; } = true;
    public int MaxLogicalOptimizationSteps { get; set; } = 50;
}
