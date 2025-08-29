using Database.Core.Execution;

namespace Database.BenchmarkRunner;

public interface IQueryRunner
{
    public void Initialize();

    public List<Row> Run(string query, CancellationToken token);
}
