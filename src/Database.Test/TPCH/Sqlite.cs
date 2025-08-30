using Database.BenchmarkRunner;

namespace Database.Test.TPCH;

[CancelAfter(10_000)]
public class SqliteTpchTests
{
    private readonly IQueryRunner _runner;

    public SqliteTpchTests()
    {
        _runner = new SqliteRunner();
        _runner.Timeout = TimeSpan.FromSeconds(10);
        _runner.Initialize();
    }

    [TestCase("query_01.sql")]
    [TestCase("query_02.sql")]
    [TestCase("query_03.sql")]
    // [TestCase("query_04.sql")]
    // [TestCase("query_05.sql")]
    [TestCase("query_06.sql")]
    [TestCase("query_07.sql")]
    [TestCase("query_08.sql")]
    [TestCase("query_09.sql")]
    [TestCase("query_10.sql")]
    [TestCase("query_11.sql")]
    [TestCase("query_12.sql")]
    [TestCase("query_13.sql")]
    [TestCase("query_14.sql")]
    [TestCase("query_15.sql")]
    [TestCase("query_16.sql")]
    // [TestCase("query_17.sql")]
    // [TestCase("query_18.sql")]
    // [TestCase("query_19.sql")]
    // [TestCase("query_20.sql")]
    // [TestCase("query_21.sql")]
    // [TestCase("query_22.sql")]
    public void Test(string test, CancellationToken token)
    {
        var query = TPCHHelpers.ReadQuery(test);
        query = _runner.Transform(query);
        _runner.Run(query, token);
    }
}
