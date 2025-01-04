using Database.Core.Execution;

namespace Database.Core.Operations;

// TODO I wonder what the engine would look like with IEnumerable/AsyncEnumerable/Task support
// using yield like would would probably be pretty nice and allow for async/io
// and allow the usage of IDisposable

// TODO I'll need to separate the logic and physical plan so we can optimize the plan
public interface IOperation
{
    RowGroup? Next();
}
