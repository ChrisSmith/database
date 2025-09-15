using Database.Core;
using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;
using Database.Core.Operations;
using Database.Core.Options;
using Database.Core.Planner;
using FluentAssertions;

namespace Database.Test;

public class CostCalculationTests
{
    private ParquetPool _bufferPool;
    private Catalog _catalog;
    private ConfigOptions _options;
    private PhysicalPlanner _physicalPlanner;
    private CostEstimation _costEstimation;
    private ExpressionBinder _binder;
    private BindContext _context;

    [OneTimeSetUp]
    public void OneTimeSetup()
    {
        _options = new ConfigOptions();
        _bufferPool = new ParquetPool();
        _catalog = new Catalog(_bufferPool);
        _costEstimation = new CostEstimation(_catalog, _bufferPool);
        _physicalPlanner = new PhysicalPlanner(_options, _catalog, _bufferPool, _costEstimation);
        _binder = new ExpressionBinder(_bufferPool, new FunctionRegistry());
        TestDatasets.AddTestDatasetsToCatalog(_catalog);
    }

    [SetUp]
    public void Setup()
    {
        _context = new BindContext();
    }

    [TestCase("l_orderkey", ExpectedResult = 3_221_559)] // Actual 1_500_000
    [TestCase("l_partkey", ExpectedResult = 200_000)]
    [TestCase("l_suppkey", ExpectedResult = 10_000)]
    [TestCase("l_linenumber", ExpectedResult = 7)]
    [TestCase("l_quantity", ExpectedResult = 2450)] // Actual 50
    [TestCase("l_extendedprice", ExpectedResult = 6_001_215)] // Actual 933_900
    [TestCase("l_discount", ExpectedResult = 11)]
    [TestCase("l_tax", ExpectedResult = 9)]
    [TestCase("l_returnflag", ExpectedResult = 76)] // Actual 3
    [TestCase("l_linestatus", ExpectedResult = 50)] // Actual 2
    [TestCase("l_shipdate", ExpectedResult = 2574)] // Actual 2526
    [TestCase("l_commitdate", ExpectedResult = 10965)] // Actual 2466
    [TestCase("l_receiptdate", ExpectedResult = 3994)] // Actual 2554
    [TestCase("l_shipinstruct", ExpectedResult = 196)] // Actual 4
    [TestCase("l_shipmode", ExpectedResult = 343)] // Actual 7
    [TestCase("l_comment", ExpectedResult = 6_001_215)] // Actual 3_610_733
    public long CardinalityEstimates_LineItem(string column)
    {
        var lineitem = _catalog.GetTable("lineitem");

        var columnRef = lineitem.Columns.Single(c => c.Name == column).ColumnRef;
        return _costEstimation.EstimateDistinctValues(columnRef).Value;
    }


    [TestCase("s_suppkey", ExpectedResult = 10_000)]
    [TestCase("s_name", ExpectedResult = 10_000)]
    [TestCase("s_address", ExpectedResult = 10_000)]
    [TestCase("s_nationkey", ExpectedResult = 25)]
    [TestCase("s_phone", ExpectedResult = 10_000)]
    [TestCase("s_acctbal", ExpectedResult = 10_000)] // Actual 9955
    [TestCase("s_comment", ExpectedResult = 10_000)] // Actual 9998
    public long CardinalityEstimates_Supplier(string column)
    {
        var lineitem = _catalog.GetTable("supplier");

        var columnRef = lineitem.Columns.Single(c => c.Name == column).ColumnRef;
        return _costEstimation.EstimateDistinctValues(columnRef).Value;
    }

    [TestCase("lineitem", ExpectedResult = 6_001_215)]
    [TestCase("orders", ExpectedResult = 1_500_000)]
    public long Scan_Logical(string table)
    {
        var plan = ScanTable(table);
        var cost = _costEstimation.Estimate(plan);
        return cost.OutputCardinality;
    }

    [Test]
    public void Scan()
    {
        var table = _catalog.GetTable("lineitem");
        var scan = new Scan(table.Name, table.Id, null, table.Columns, Cardinality: table.NumRows);
        var plan = _physicalPlanner.CreatePhysicalPlan(scan, new BindContext());
        var cost = plan.EstimateCost();
        cost.OutputRows.Should().Be(6_001_215);
        cost.TotalCpuOperations.Should().Be(6_001_215);
        cost.TotalDiskOperations.Should().Be(49);
        cost.TotalCost().Should().Be(6_001 + 4);
    }

    [Test]
    public void Filter()
    {
        LogicalPlan plan = ScanTable("lineitem");
        // TODO add a selectivity test on a column we have basic cardinality counts on
        plan = new Filter(plan, new BinaryExpression(TokenType.EQUAL, "=", new IntegerLiteral(0), new IntegerLiteral(0)));

        var cost = LogicalCost(plan);
        cost.OutputCardinality.Should().Be(6_001_215 / 5);

        var pcost = PhysicalCost(plan);
        pcost.OutputRows.Should().Be(6_001_215 / 5);
        pcost.TotalCpuOperations.Should().Be(6_001_215 + (6_001_215 * plan.OutputSchema.Count));
        pcost.TotalDiskOperations.Should().Be(49);
        pcost.TotalCost().Should().Be(102024);
    }

    [TestCase("region", "nation", "n_regionkey = r_regionkey", 25)]
    [TestCase("lineitem", "orders", "l_orderkey = o_orderkey", 6_001_215)]
    public void Join(string left, string right, string joinExpr, int cardinality)
    {
        var leftT = ScanTable(left);
        var rightT = ScanTable(right);
        var joinLR = InnerJoin(leftT, rightT, joinExpr);
        var joinRL = InnerJoin(rightT, leftT, joinExpr);

        var costLR = LogicalCost(joinLR);
        var costRL = LogicalCost(joinRL);
        costLR.OutputCardinality.Should().Be(costRL.OutputCardinality, "Join ordering doesn't affect number of tuples output");
        costLR.OutputCardinality.Should().Be(cardinality, "Entire domain of FK is assumed to be a subset of PK, so total size is the size of the larger table");

        var pcostLR = PhysicalCost(joinLR);
        var pcostRL = PhysicalCost(joinRL);
        // TODO I wonder if the equality here is an issue
        // we lose precision for very low cost plans (ops / 1000)
        pcostLR.TotalCost().Should().BeLessThanOrEqualTo(pcostRL.TotalCost(), "Cheaper to have the smaller table on the build side of the join");
    }

    public enum TestType { Filter, Join }
    public record CardinalityTest(TestType Type, string Table, int Cardinality, string? JoinExpr = null, string? FilterExpr = null);

    [TestCaseSource(nameof(TestCases))]
    public void JoinWithFilter(CardinalityTest[] actions)
    {
        LogicalPlan? plan = null;
        for (var i = 0; i < actions.Length; i++)
        {
            var next = actions[i];

            if (next is { Type: TestType.Filter })
            {
                plan = Filter(plan!, next.FilterExpr!);
            }
            else if (next is { Type: TestType.Join })
            {
                LogicalPlan newTable = ScanTable(next.Table);
                if (next.FilterExpr != null)
                {
                    newTable = Filter(newTable, next.FilterExpr!);
                }

                if (plan == null)
                {
                    plan = newTable;
                    continue;
                }
                plan = InnerJoin(newTable, plan, next.JoinExpr!);
            }
            else
            {
                throw new Exception("Unknown test type");
            }

            var cost = LogicalCost(plan);
            var expectedCardinality = next.Cardinality;
            cost.OutputCardinality.Should().Be(expectedCardinality);
        }
    }

    private static CardinalityTest[][] TestCases()
    {
        return
        [
            // Join order from Duckdb
            [
                new CardinalityTest(TestType.Join, "region", 1, null, "r_name='ASIA'"),
                new CardinalityTest(TestType.Join, "nation", 5, "n_regionkey = r_regionkey"),
                new CardinalityTest(TestType.Join, "customer", 30_000, "c_nationkey = n_nationkey"),
                new CardinalityTest(TestType.Join, "orders", 60_000, "o_custkey = c_custkey", "o_orderdate >= DATE '1994-01-01'"),
                new CardinalityTest(TestType.Join, "lineitem", 240_048, "l_orderkey = o_orderkey"),
                new CardinalityTest(TestType.Join, "supplier", 240_048, "l_suppkey = s_suppkey"),
                new CardinalityTest(TestType.Filter, "", 9_601, null, "c_nationkey = s_nationkey"),
            ],

            // Bad plan my db was originally choosing
            [
                new CardinalityTest(TestType.Join, "orders", 300_000, null, "o_orderdate >= DATE '1994-01-01'"),
                new CardinalityTest(TestType.Join, "customer", 300_002, "o_custkey = c_custkey"),
                new CardinalityTest(TestType.Join, "supplier", 120_000_800,  "s_nationkey = c_nationkey"),
                new CardinalityTest(TestType.Join, "lineitem", 480_100_400, "l_orderkey = o_orderkey"),
                new CardinalityTest(TestType.Filter, "", 48_010, null,  "l_suppkey = s_suppkey"),
                new CardinalityTest(TestType.Join, "nation", 48_010, "c_nationkey = n_nationkey"),
                new CardinalityTest(TestType.Join, "region", 9_602, "n_regionkey = r_regionkey", "r_name='ASIA'"),
            ],
        ];
    }


    private Cost PhysicalCost(LogicalPlan plan)
    {
        var physicalPlan = _physicalPlanner.CreatePhysicalPlan(plan, _context);
        return physicalPlan.EstimateCost();
    }

    private CostEstimate LogicalCost(LogicalPlan plan)
    {
        return _costEstimation.Estimate(plan);
    }

    private Filter Filter(LogicalPlan plan, string filterExpr)
    {
        var bound = _binder.Bind(_context, Expr(filterExpr), plan.OutputSchema);
        var filter = new Filter(plan, bound);
        return filter;
    }
    private Join InnerJoin(LogicalPlan left, LogicalPlan right, string joinExpr)
    {
        var bound = _binder.Bind(_context, BinaryExpr(joinExpr), QueryPlanner.ExtendSchema(left.OutputSchema, right.OutputSchema));
        var join = new Join(left, right, JoinType.Inner, bound);
        return join;
    }

    private Scan ScanTable(string name)
    {
        var table = _catalog.GetTable(name);
        return new Scan(table.Name, table.Id, null, table.Columns, Cardinality: table.NumRows);
    }

    private BaseExpression Expr(string str)
    {
        var scanner = new Scanner(str);
        var tokens = scanner.ScanTokens();

        var parser = new Parser(tokens);
        return parser.ParseExpr();
    }

    private BinaryExpression BinaryExpr(string str)
    {
        var result = Expr(str);
        return (BinaryExpression)result;
    }
}
