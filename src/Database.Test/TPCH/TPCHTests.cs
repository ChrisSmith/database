using Database.Core;
using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Planner;

namespace Database.Test.TPCH;

public partial class TPCHTests
{
    private Catalog _catalog;

    [OneTimeSetUp]
    public void OneTimeSetup()
    {
        _catalog = new Catalog(new ParquetPool());
        TestDatasets.AddTestDatasetsToCatalog(_catalog);
    }

    protected List<MaterializedRowGroup> Query(string query)
    {
        var scanner = new Scanner(query);
        var tokens = scanner.ScanTokens();
        var parser = new Parser(tokens);
        var statement = parser.Parse();

        var bufferPool = new ParquetPool();
        var it = new Interpreter(bufferPool);
        var planner = new QueryPlanner(_catalog, bufferPool);
        var plan = planner.CreatePlan(statement);
        var result = it.Execute(plan).ToList();
        return result;
    }
}
