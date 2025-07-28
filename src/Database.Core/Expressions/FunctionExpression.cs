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
}
