using System.Diagnostics;

namespace Database.Core.Expressions;

[DebuggerDisplay("{Name}({Args})}")]
public record FunctionExpression(string Name, params IExpression[] Args) : BaseExpression;
