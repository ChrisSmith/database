using Database.Core.Execution;

namespace Database.BenchmarkRunner;

public interface IQueryRunner
{
    public void Initialize();

    public string Transform(string query);

    public List<Row> Run(string query, CancellationToken token);

    public List<Row> Run(string query, CancellationToken token, out TimeSpan? duration);

    public TimeSpan Timeout { get; set; }
}
