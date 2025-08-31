using System.Diagnostics;

namespace Database.Core.Expressions;

[DebuggerDisplay("{Column}")]
public record ColumnExpression(string Column, string? Table = null) : BaseExpression
{
    public override IEnumerable<BaseExpression> Children()
    {
        yield break;
    }

    public override string ToString()
    {
        return Column;
    }

    protected override BaseExpression WithChildren(IReadOnlyList<BaseExpression> newChildren)
    {
        throw new NotSupportedException($"{GetType().Name} does not support replacing children.");
    }

    public static ColumnExpression FromString(string str)
    {
        if (str.Contains('.'))
        {
            var parts = str.Split('.');
            if (parts.Length != 2)
            {
                throw new ArgumentException($"Invalid column expression: {str}");
            }
            return new ColumnExpression(parts[1], parts[0]);
        }

        return new ColumnExpression(str);
    }
}
