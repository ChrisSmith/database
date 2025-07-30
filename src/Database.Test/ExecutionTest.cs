using System.Diagnostics;
using Database.Core;
using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Planner;
using FluentAssertions;

namespace Database.Test;

public class ExecutionTest
{
    private ParquetPool _bufferPool;
    private Catalog _catalog;

    [OneTimeSetUp]
    public void OneTimeSetup()
    {
        _bufferPool = new ParquetPool();
        _catalog = new Catalog(_bufferPool);
        TestDatasets.AddTestDatasetsToCatalog(_catalog);
    }

    private List<MaterializedRowGroup> Query(string query)
    {
        var scanner = new Scanner(query);
        var tokens = scanner.ScanTokens();
        var parser = new Parser(tokens);
        var statement = parser.Parse();

        var it = new Interpreter(_bufferPool);
        var planner = new QueryPlanner(_catalog, _bufferPool);
        var plan = planner.CreatePlan(statement);
        var result = it.Execute(plan).ToList();
        return result;
    }

    [Test]
    public void Select_All()
    {
        var result = Query("""
            SELECT
                   Id
                 , Unordered
                 , Name
                 , CategoricalInt
                 , CategoricalString
            FROM table;
        """);

        result.Should().HaveCount(10);
        var rg = result[0];
        rg.Columns.Select(c => c.Name).Should().BeEquivalentTo(new List<string>
        {
            "Id", "Unordered", "Name", "CategoricalInt", "CategoricalString"
        });
        rg.Columns.Should().HaveCount(5);
        rg.Columns[0].Should().BeOfType<Column<int>>();
        rg.Columns[1].Should().BeOfType<Column<int>>();
        rg.Columns[2].Should().BeOfType<Column<string>>();
        rg.Columns[3].Should().BeOfType<Column<int>>();
        rg.Columns[4].Should().BeOfType<Column<string>>();

        var column = (Column<int>)rg.Columns[0];
        column.Values.Should().HaveCount(10_000);
    }

    [Test]
    public void Select_Distinct()
    {
        var result = Query("SELECT distinct CategoricalInt FROM table;").AsRowList();

        result.Should().HaveCount(5);
        var values = result.Select(r => r.Values[0]).OrderBy(r => r).ToList();
        values.Should().HaveCount(5);
        values.Should().BeEquivalentTo(new List<int> { 0, 1, 2, 3, 4 });
    }

    [TestCase("Id * Id")]
    [TestCase("Id")]
    [TestCase("Id + 1")]
    [TestCase("Id * 2")]
    [TestCase("Id % 2")]
    [TestCase("Id / 1")]
    [TestCase("Id / 2")]
    [TestCase("Id / 8")]
    [TestCase("Id + 1 + 1")]
    [TestCase("Id + 1 * 3")]
    [TestCase("0 + 1 * 3")]
    [TestCase("0 + 1 * 6 / 2")]
    [TestCase("0 as foo")]
    [TestCase("0 + 1 as foo")]
    // [TestCase("Id / 8.0")] // This doesn't work yet because we don't have upcasts
    public void Select_Expressions(string expr)
    {
        var result = Query($"SELECT {expr} FROM table").AsRowList();

        var res = (double)Convert.ChangeType(result[0].Values[0], typeof(double))!;
        res.Should().BeGreaterThanOrEqualTo(0);
    }

    [TestCase("Id * Id", ExpectedResult = 100)]
    [TestCase("Id", ExpectedResult = 10)]
    [TestCase("Id + 1", ExpectedResult = 11)]
    [TestCase("Id * 2", ExpectedResult = 20)]
    [TestCase("Id % 2", ExpectedResult = 0)]
    [TestCase("Id / 1", ExpectedResult = 10)]
    [TestCase("Id / 2", ExpectedResult = 5)]
    [TestCase("Id / 8", ExpectedResult = 1)]
    [TestCase("Id + 1 + 1", ExpectedResult = 12)]
    [TestCase("Id + 1 * 3", ExpectedResult = 13)]
    [TestCase("0 + 1 * 3", ExpectedResult = 3)]
    [TestCase("0 + 1 * 6 / 2", ExpectedResult = 3)]
    [TestCase("0 as foo", ExpectedResult = 0)]
    [TestCase("0 + 1 as foo", ExpectedResult = 1)]
    // [TestCase("Id / 8.0", ExpectedResult = 1.25)] // This doesn't work yet because we don't have upcasts
    public object Select_Expressions_With_Filter(string expr)
    {
        var result = Query($"SELECT {expr} FROM table where Id = 10;").AsRowList();

        result.Should().HaveCount(1);
        return result[0].Values[0]!;
    }

    [Test]
    public void Select_Expressions_Reference_In_Where()
    {
        var result = Query($"SELECT Id + 1 as foo FROM table where foo = 11;").AsRowList();

        result.Should().HaveCount(1);
        result[0].Values[0]!.Should().Be(11);
    }

    [Test]
    public void Select_Aggregations()
    {
        // SELECT
        // count(*) as count
        //     , count(Id) as count_id
        //     , sum(1) as sum_1
        //     , sum(CategoricalInt) as sum_cat_int
        //
        // FROM table;
        //
        var result = Query("""
                               SELECT
                                      count(Id) as count_id
                                    , count(CategoricalInt) as count_cat_int
                                    , sum(CategoricalInt) as sum_cat_int
                                    , avg(CategoricalInt) as avg_cat_int
                               FROM table;
                           """);

        result.Should().HaveCount(1);
        var rg = result[0];
        rg.Columns.Select(c => c.Name).Should().BeEquivalentTo(new List<string>
        {
            "count_id",
            "count_cat_int",
            "sum_cat_int",
            "avg_cat_int",

            // "count",
            // "sum_1",
        });
        rg.Columns[0].Should().BeOfType<Column<int>>();
        rg.Columns[1].Should().BeOfType<Column<int>>();
        rg.Columns[2].Should().BeOfType<Column<int>>();
        rg.Columns[3].Should().BeOfType<Column<double>>();


        rg.Columns[0][0].Should().Be(100_000);
        rg.Columns[1][0].Should().Be(100_000);
        rg.Columns[2][0].Should().Be(199_549);
        rg.Columns[3][0].Should().Be(1.99549);

        // rg.Columns[3].Should().BeOfType<Column<int>>();
        // rg.Columns[4].Should().BeOfType<Column<double>>();
    }

    // passing
    [TestCase("Id < 100")]
    [TestCase("Id <= 99")]
    [TestCase("100 > Id")]
    [TestCase("99 >= Id")]
    [TestCase("Id > 99899")]
    [TestCase("Id > 100000 - 101")]
    [TestCase("99899 < Id")]
    [TestCase("100000 - 101 < Id")]
    [TestCase("Id between 0 and 99")]
    public void Where(string expr)
    {
        var result = Query($"SELECT Id FROM table where {expr};").AsRowList();
        result.Select(r => r.Values.Single()).ToList().Should().HaveCount(100);
    }

    [TestCase("Id < 110 and Id >= 10")]
    [TestCase("Id <= 10 or Id > 100000 - 90")]
    [TestCase("not Id <= 99899")]
    public void Where_CompoundExpressions(string expr)
    {
        var result = Query($"SELECT Id FROM table where {expr};").AsRowList();
        result.Select(r => r.Values.Single()).ToList().Should().HaveCount(100);
    }

    [TestCase("select Id from table")]
    [TestCase("select Id / 2 as foo from table")]
    [TestCase("select 1 + 1 from table")]
    [TestCase("select Id + 1 from table")]
    [TestCase("select Id + 1 as foo from table")]
    [TestCase("select count(Id), sum(Id) from table")]
    [TestCase("select sum(Id) from table")]
    [TestCase("select sum(Id) as foo from table")]
    [TestCase("select CategoricalInt from table")]
    [TestCase("select distinct CategoricalInt from table")]
    [TestCase("select Id + 1 + 2 from table")]
    [TestCase("select Id + 1 + 2 as foo from table")]
    [TestCase("select Id as foo from table")]
    // Not working yet
    // The easiest way to support this is probably query re-writing with aliases
    // That is likely also required to support expression de-duplication
    // [TestCase("select count(Id) + sum(Id) from table")]
    // [TestCase("select sum(Id) / 2 as foo from table")]
    public void ValidateQueryDoesntCrash(string query)
    {
        var result = Query(query + ";").AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }
}
