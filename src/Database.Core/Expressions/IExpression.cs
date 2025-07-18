using Database.Core.Catalog;
using Database.Core.Functions;

namespace Database.Core.Expressions;

public interface IExpression
{
    // Index of the expression in its output from the prior operator
    public int BoundIndex { get; set; }

    public DataType? BoundDataType { get; set; }

    public IFunction? BoundFunction { get; set; }

    public string Alias { get; set; }
}

public record BaseExpression : IExpression
{
    public int BoundIndex { get; set; } = -1;

    public DataType? BoundDataType { get; set; }

    public IFunction? BoundFunction { get; set; }

    public string Alias { get; set; } = string.Empty;
}
