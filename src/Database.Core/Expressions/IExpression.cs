using Database.Core.Catalog;

namespace Database.Core.Expressions;

public interface IExpression
{
    public int BoundIndex { get; set; }

    public DataType? BoundDataType { get; set; }

    public string Alias { get; set; }
}

public record BaseExpression : IExpression
{
    public int BoundIndex { get; set; } = -1;

    public DataType? BoundDataType { get; set; }

    public string Alias { get; set; } = string.Empty;
}
