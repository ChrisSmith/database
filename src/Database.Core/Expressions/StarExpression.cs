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
}
