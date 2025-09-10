using Database.Core;
using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;
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

    [OneTimeSetUp]
    public void OneTimeSetup()
    {
        _options = new ConfigOptions();
        _bufferPool = new ParquetPool();
        _catalog = new Catalog(_bufferPool);
        _physicalPlanner = new PhysicalPlanner(_options, _catalog, _bufferPool);
        TestDatasets.AddTestDatasetsToCatalog(_catalog);
    }

    [Test]
    public void Scan()
    {
        var table = _catalog.GetTable("lineitem");
        var scan = new Scan(table.Name, table.Id, null, table.Columns, NumRows: 10);
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
        var table = _catalog.GetTable("lineitem");
        var scan = new Scan(table.Name, table.Id, null, table.Columns, NumRows: 10);
        var filter = new Filter(scan, new BinaryExpression(TokenType.EQUAL, "=", new IntegerLiteral(0), new IntegerLiteral(0)));
        var plan = _physicalPlanner.CreatePhysicalPlan(filter, new BindContext());
        var cost = plan.EstimateCost();
        cost.OutputRows.Should().Be(6_001_215 / 10);
        cost.TotalCpuOperations.Should().Be(6_001_215 + (6_001_215 * table.Columns.Count));
        cost.TotalDiskOperations.Should().Be(49);
        cost.TotalCost().Should().Be(102024);
    }

    [Test]
    public void Join()
    {
        var table = _catalog.GetTable("lineitem");
        LogicalPlan smaller = new Scan(table.Name, table.Id, null, table.Columns, NumRows: 10, Alias: "left");
        var expr = new BinaryExpression(TokenType.EQUAL, "=", new IntegerLiteral(0), new IntegerLiteral(0));
        smaller = new Filter(smaller, expr);

        var larger = new Scan(table.Name, table.Id, null, table.Columns, NumRows: 10, Alias: "right");
        var joinExpr = new BinaryExpression(TokenType.EQUAL, "=",
            new ColumnExpression("l_orderkey", "left"),
            new ColumnExpression("l_orderkey", "right"));

        var join = new Join(smaller, larger, JoinType.Inner, joinExpr);

        var plan = _physicalPlanner.CreatePhysicalPlan(join, new BindContext());
        var cost = plan.EstimateCost();
        cost.OutputRows.Should().Be(6_001_215);
        cost.TotalCpuOperations.Should().Be(120_624_421);
        cost.TotalDiskOperations.Should().Be(98);
        cost.TotalCost().Should().Be(120_633);


        join = new Join(larger, smaller, JoinType.Inner, joinExpr);

        plan = _physicalPlanner.CreatePhysicalPlan(join, new BindContext());
        cost = plan.EstimateCost();
        cost.OutputRows.Should().Be(6_001_215);
        cost.TotalCpuOperations.Should().Be(115_223_327);
        cost.TotalDiskOperations.Should().Be(98);
        cost.TotalCost().Should().Be(115_232); // smaller than when the large table is on the left
    }
}
