using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Functions;

namespace Database.Core.Expressions;

public interface IExpression
{
    /// <summary>
    /// The output location for the expression
    /// If the expression is a simple select statement, this can also be the input column
    /// Otherwise, a new column will be allocated in the buffer pool and written to.
    /// Not all expressions will be bound, intermediate results from complex expressions
    /// are not written back to the buffer pool atm.
    /// </summary>
    public ColumnRef BoundOutputColumn { get; set; }

    public DataType? BoundDataType { get; set; }

    public IFunction? BoundFunction { get; set; }

    public string Alias { get; set; }
}

public record BaseExpression : IExpression
{
    public ColumnRef BoundOutputColumn { get; set; }

    public DataType? BoundDataType { get; set; }

    public IFunction? BoundFunction { get; set; }

    public string Alias { get; set; } = string.Empty;
}
