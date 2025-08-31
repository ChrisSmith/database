using System.Text;

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
        var condCount = Conditions.Count;
        if (newChildren.Count != condCount + Results.Count + (Default != null ? 1 : 0))
        {
            throw new ArgumentException($"CaseExpression expects {condCount + Results.Count + (Default != null ? 1 : 0)} children but received {newChildren.Count}.");
        }
        return this with
        {
            Conditions = [.. newChildren.Take(condCount)],
            Results = [.. newChildren.Skip(condCount).Take(Results.Count)],
            Default = newChildren.Count > condCount + Results.Count ? newChildren[^1] : null
        };
    }

    public override string ToString()
    {
        var builder = new StringBuilder("CASE(");
        for (var i = 0; i < Conditions.Count; i++)
        {
            var condition = Conditions[i];
            var result = Results[i];
            builder.Append($"WHEN {condition} THEN {result}");
        }
        builder.Append(')');
        return builder.ToString();
    }
}
