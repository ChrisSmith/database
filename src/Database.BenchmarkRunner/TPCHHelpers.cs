using Database.Core;
using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Options;
using Database.Core.Planner;

namespace Database.BenchmarkRunner;

public class TPCHHelpers
{
    private Catalog _catalog;
    private ParquetPool _bufferPool;
    private ConfigOptions _options;

    public TPCHHelpers()
    {
        _options = new ConfigOptions();
        _bufferPool = new ParquetPool();
        _catalog = new Catalog(_bufferPool);
        TestDatasets.AddTestDatasetsToCatalog(_catalog);
    }

    public static string ReadQuery(string name)
    {
        return File.ReadAllText($"Queries/{name}");
    }
}
