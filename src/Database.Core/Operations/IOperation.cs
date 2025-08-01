using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;

namespace Database.Core.Operations;

// TODO I wonder what the engine would look like with IEnumerable/AsyncEnumerable/Task support
// using yield like would would probably be pretty nice and allow for async/io
// and allow the usage of IDisposable

// TODO I'll need to separate the logic and physical plan so we can optimize the plan
public interface IOperation
{
    IReadOnlyList<ColumnSchema> Columns { get; }

    IReadOnlyList<ColumnRef> ColumnRefs { get; }

    RowGroup? Next();
}

public abstract record BaseOperation(
    IReadOnlyList<ColumnSchema> Columns,
    IReadOnlyList<ColumnRef> ColumnRefs) : IOperation
{
    public abstract RowGroup? Next();
}
