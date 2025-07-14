using System.Diagnostics;

namespace Database.Core.Expressions;

[DebuggerDisplay("{Column}")]
public record ColumnExpression(string Column, string? Table = null) : BaseExpression
{

}
