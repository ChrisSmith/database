using Database.Core;
using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Functions;
using Database.Core.Planner;
using FluentAssertions;
using static Database.Test.TestUtils;

namespace Database.Test;

public class QueryOptimizerTests
{
    private ParquetPool _bufferPool;
    private Catalog _catalog;
    private QueryOptimizer _optimizer;
    private ExplainQuery _explain;

    [OneTimeSetUp]
    public void Setup()
    {
        _bufferPool = new ParquetPool();
        _catalog = new Catalog(_bufferPool);
        _optimizer = new QueryOptimizer(new ExpressionBinder(_bufferPool, new FunctionRegistry()));
        _explain = new ExplainQuery();
        TestDatasets.AddTestDatasetsToCatalog(_catalog);
    }

    private static readonly VerifySettings Settings = new();
    static QueryOptimizerTests()
    {
        Settings.UseDirectory("Snapshots/Optimizer/");
        Settings.IgnoreMembersWithType<ColumnRef>();
        Settings.IgnoreMembersWithType<ColumnSchema>();
        Settings.IgnoreMembersWithType<IReadOnlyList<ColumnSchema>>();
        Settings.AutoVerify();
    }

    private LogicalPlan Plan(string query)
    {
        var scanner = new Scanner(query);
        var tokens = scanner.ScanTokens();
        var parser = new Parser(tokens);
        var statement = parser.Parse();

        var it = new Interpreter(_bufferPool);
        var planner = new QueryPlanner(_catalog, _bufferPool);
        return planner.CreateLogicalPlan(statement);
    }

    private string OptimizeAndExplain(string query)
    {
        var plan = Plan(query);
        var optimized = _optimizer.OptimizeBlah(plan);

        var diff = $"{query}\n\nOriginal\n{_explain.Explain(plan)}\n\nOptimized\n{_explain.Explain(optimized)}\n\n";
        return diff;
    }

    [TestCase("select * FROM table t1")]
    [TestCase("select Id FROM table t1")]
    public Task SimplePlan(string query)
    {
        var plan = OptimizeAndExplain(query);
        return Verify(plan, Settings).UseParameters(CleanStringForFileName(query));
    }

    [TestCase("select Id FROM table t1 where t1.Id = 1")]
    [TestCase("select t1.Id FROM table t1 join table t2 on t1.Id = t2.Id where t1.Id = 1")]
    [TestCase("select t1.Id FROM table t1, table t2 where t1.Id = t2.Id and t1.Id = 1")]
    [TestCase("select t1.Id FROM table t1, table t2 where t1.Id = t1.CategoricalInt and t1.Id = 1")]
    public Task PushDownPredicate(string query)
    {
        var plan = OptimizeAndExplain(query);
        return Verify(plan, Settings).UseParameters(CleanStringForFileName(query));
    }

}
