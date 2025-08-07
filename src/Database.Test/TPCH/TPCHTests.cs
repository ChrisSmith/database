using Database.Core;
using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Options;
using Database.Core.Planner;

namespace Database.Test.TPCH;

public partial class TPCHTests
{
    private Catalog _catalog;
    private ParquetPool _bufferPool;
    private ConfigOptions _options;

    [OneTimeSetUp]
    public void OneTimeSetup()
    {
        _options = new ConfigOptions();
        _bufferPool = new ParquetPool();
        _catalog = new Catalog(_bufferPool);
        TestDatasets.AddTestDatasetsToCatalog(_catalog);
    }

    protected List<MaterializedRowGroup> Query(string query)
    {
        var scanner = new Scanner(query);
        var tokens = scanner.ScanTokens();
        var parser = new Parser(tokens);
        var statement = parser.Parse();

        var it = new Interpreter(_bufferPool);
        var planner = new QueryPlanner(_options, _catalog, _bufferPool);
        var plan = planner.CreatePlan(statement.Statement);
        var result = it.Execute(plan).ToList();
        return result;
    }

    protected string ReadQuery(string name)
    {
        return File.ReadAllText($"TPCH/Queries/{name}");
    }
}
