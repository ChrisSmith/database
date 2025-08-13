using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Functions;

namespace Database.Core.Expressions;

/// <summary>
/// The output location for the expression
/// If the expression is a simple select statement, this can also be the input column
/// Otherwise, a new column will be allocated in the buffer pool and written to.
/// Not all expressions will be bound, intermediate results from complex expressions
/// are not written back to the buffer pool atm.
/// </summary>
public abstract record BaseExpression(
    ColumnRef BoundOutputColumn = default,
    DataType? BoundDataType = null,
    IFunction? BoundFunction = null,
    string Alias = ""
    )
{
    public abstract IEnumerable<BaseExpression> Children();

    public bool AnyChildOrSelf(Predicate<BaseExpression> predicate)
    {
        if (predicate(this))
        {
            return true;
        }

        foreach (var child in Children())
        {
            if (predicate(child))
            {
                return true;
            }
        }

        return false;
    }

    public void Walk(Action<BaseExpression> fun)
    {
        fun(this);
        foreach (var child in Children())
        {
            child.Walk(fun);
        }
    }

    protected abstract BaseExpression WithChildren(IReadOnlyList<BaseExpression> newChildren);

    public BaseExpression Rewrite(Func<BaseExpression, BaseExpression?> rewriter)
    {
        var replaced = rewriter(this);
        if (replaced is not null && !ReferenceEquals(replaced, this))
        {
            // If this node is replaced, continue rewriting inside the replacement
            return replaced.Rewrite(rewriter);
        }

        var existingChildren = Children().ToArray();
        var newChildren = new BaseExpression[existingChildren.Length];
        var anyChanged = false;
        for (var i = 0; i < existingChildren.Length; i++)
        {
            var rewrittenChild = existingChildren[i].Rewrite(rewriter);
            newChildren[i] = rewrittenChild;
            if (!ReferenceEquals(rewrittenChild, existingChildren[i]))
            {
                anyChanged = true;
            }
        }

        return anyChanged ? WithChildren(newChildren) : this;
    }
}
