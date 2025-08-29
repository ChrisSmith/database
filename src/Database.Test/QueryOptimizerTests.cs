using Database.Core;
using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Functions;
using Database.Core.Options;
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
    private BindContext _context;
    private ConfigOptions _options;

    [OneTimeSetUp]
    public void Setup()
    {
        _options = new ConfigOptions();
        _bufferPool = new ParquetPool();
        _catalog = new Catalog(_bufferPool);
        _optimizer = new QueryOptimizer(_options, new ExpressionBinder(_bufferPool, new FunctionRegistry()), _bufferPool);
        _explain = new ExplainQuery(_options);
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
        var planner = new QueryPlanner(_options, _catalog, _bufferPool);
        _context = new BindContext();
        return planner.CreateLogicalPlan(statement.Statement, _context);
    }

    private string OptimizeAndExplain(string query)
    {
        var plan = Plan(query);
        var optimized = _optimizer.OptimizePlan(plan, _context);

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

    [Test]
    public Task CrossJoinReordering()
    {
        var query = @"
            select
                l_orderkey,
                sum(l_extendedprice*(1-l_discount)) as revenue,
                o_orderdate,
                o_shippriority
            from customer, orders, lineitem
            where
                    c_mktsegment = 'BUILDING'
                and c_custkey = o_custkey
                and l_orderkey = o_orderkey
                and o_orderdate < date '1995-03-15'
                and l_shipdate > date '1995-03-15'
            group by l_orderkey, o_orderdate, o_shippriority
            order by revenue desc, o_orderdate
            limit 10;
        ";
        var plan = OptimizeAndExplain(query);
        return Verify(plan, Settings);
    }

    [Test]
    public Task SubQuery_UnCorrelated_Scalar()
    {
        var query = @"
            select count(*) as count
            from table t
            where t.CategoricalInt = (
                select max(CategoricalInt)
                from table q
                where q.CategoricalString = 'cat'
            )
        ";
        var plan = OptimizeAndExplain(query);
        return Verify(plan, Settings);
    }

    [TestCase("query_01.sql")]
    [TestCase("query_02.sql")]
    [TestCase("query_03.sql")]
    [TestCase("query_04.sql")]
    [TestCase("query_05.sql")]
    [TestCase("query_06.sql")]
    [TestCase("query_07.sql")]
    [TestCase("query_08.sql")]
    [TestCase("query_09.sql")]
    [TestCase("query_10.sql")]
    [TestCase("query_11.sql")]
    [TestCase("query_12.sql")]
    [TestCase("query_13.sql")]
    [TestCase("query_14.sql")]
    /*
    [TestCase("query_15.sql")]
    [TestCase("query_16.sql")]
    */
    [TestCase("query_17.sql")]
    [TestCase("query_18.sql")]
    /*
    [TestCase("query_19.sql")]
    [TestCase("query_20.sql")]
    [TestCase("query_21.sql")]
    [TestCase("query_22.sql")]
    */
    public Task TPCH_Queries(string name)
    {
        var query = File.ReadAllText(Path.Combine("Queries", name));
        var plan = OptimizeAndExplain(query);
        return Verify(plan, Settings);
    }
}
