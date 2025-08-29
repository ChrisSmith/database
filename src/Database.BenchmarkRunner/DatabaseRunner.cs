using Database.Core;
using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Options;
using Database.Core.Planner;

namespace Database.BenchmarkRunner;

public class DatabaseRunner : IQueryRunner
{
    private Catalog _catalog;
    private ParquetPool _bufferPool;
    private ConfigOptions _options;
    private Interpreter _it;
    private QueryPlanner _planner;

    public DatabaseRunner()
    {
        _options = new ConfigOptions();
        _bufferPool = new ParquetPool();
        _catalog = new Catalog(_bufferPool);
        _it = new Interpreter(_bufferPool);
        _planner = new QueryPlanner(_options, _catalog, _bufferPool);
    }

    public TimeSpan Timeout { get; set; }

    public void Initialize()
    {
        TestDatasets.AddTestDatasetsToCatalog(_catalog);
    }

    public string Transform(string query)
    {
        return query;
    }

    public List<Row> Run(string query, CancellationToken token)
    {
        var scanner = new Scanner(query);
        var tokens = scanner.ScanTokens();
        var parser = new Parser(tokens);
        var statement = parser.Parse();

        var plan = _planner.CreatePlan(statement.Statement);
        var result = _it.Execute(plan, token).ToList().AsRowList();
        return result;
    }
}
