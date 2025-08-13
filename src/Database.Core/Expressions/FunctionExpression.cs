using System.Diagnostics;

namespace Database.Core.Expressions;

[DebuggerDisplay("{Name}({Args})}")]
public record FunctionExpression(string Name, params BaseExpression[] Args) : BaseExpression
{
    public override IEnumerable<BaseExpression> Children()
    {
        for (var i = 0; i < Args.Length; i++)
        {
            var arg = Args[i];
            yield return arg;
        }
    }

    public override string ToString()
    {
        var argsStr = string.Join<BaseExpression>(", ", Args);
        return $"{Name}({argsStr})";
    }

    protected override BaseExpression WithChildren(IReadOnlyList<BaseExpression> newChildren)
    {
        if (newChildren.Count != Args.Length)
        {
            throw new ArgumentException($"FunctionExpression expects {Args.Length} children but received {newChildren.Count}.");
        }

        var newArgs = new BaseExpression[newChildren.Count];
        for (var i = 0; i < newChildren.Count; i++)
        {
            newArgs[i] = newChildren[i];
        }
        return this with { Args = newArgs };
    }
}
