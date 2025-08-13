namespace Database.Core.Expressions;

public record StarExpression(string? Table = null) : BaseExpression
{
    public override IEnumerable<BaseExpression> Children()
    {
        yield break;
    }

    public override string ToString()
    {
        return "*";
    }

    protected override BaseExpression WithChildren(IReadOnlyList<BaseExpression> newChildren)
    {
        throw new NotSupportedException($"{GetType().Name} does not support replacing children.");
    }
}
