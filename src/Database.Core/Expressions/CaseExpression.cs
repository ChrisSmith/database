namespace Database.Core.Expressions;

public record CaseExpression(List<BaseExpression> Conditions, List<BaseExpression> Results, BaseExpression? Default) : BaseExpression
{
    public override IEnumerable<BaseExpression> Children()
    {
        foreach (var condition in Conditions)
        {
            yield return condition;
        }

        foreach (var result in Results)
        {
            yield return result;
        }

        if (Default != null)
        {
            yield return Default;
        }
    }

    protected override BaseExpression WithChildren(IReadOnlyList<BaseExpression> newChildren)
    {
        throw new NotImplementedException();
    }
}
