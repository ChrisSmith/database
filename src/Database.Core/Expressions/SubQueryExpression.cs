namespace Database.Core.Expressions;

public record SubQueryExpression(SelectStatement Select, bool ExistsOnly) : BaseExpression()
{
    public override IEnumerable<BaseExpression> Children()
    {
        yield break;
    }

    protected override BaseExpression WithChildren(IReadOnlyList<BaseExpression> newChildren)
    {
        throw new NotImplementedException();
    }
}
