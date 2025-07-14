using System.Diagnostics;
using Database.Core;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Planner;
using FluentAssertions;

namespace Database.Test;

public class ExecutionTest
{
    private Catalog _catalog;


    [OneTimeSetUp]
    public void OneTimeSetup()
    {
        var homeDir = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        var dataPath = Path.Combine(homeDir, "src/database/data.parquet");
        _catalog = new Catalog();
        _catalog.Tables.Add(new TableSchema((TableId)1, "table", new List<ColumnSchema>
        {
            new((ColumnId)1, "Id", DataType.Int, typeof(int)),
            new((ColumnId)2, "Unordered", DataType.Int, typeof(int)),
            new((ColumnId)3, "Name", DataType.String, typeof(string)),
            new((ColumnId)4, "CategoricalInt", DataType.Int, typeof(int)),
            new((ColumnId)5, "CategoricalString", DataType.String, typeof(string)),
        }, dataPath));
    }

    private List<RowGroup> Query(string query)
    {
        var scanner = new Scanner(query);
        var tokens = scanner.ScanTokens();
        var parser = new Parser(tokens);
        var statement = parser.Parse();

        var it = new Interpreter();
        var planner = new QueryPlanner(_catalog);
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

    [TestCase("Id", ExpectedResult = 10)]
    [TestCase("Id + 1", ExpectedResult = 11)]
    [TestCase("Id * 2", ExpectedResult = 20)]
    [TestCase("Id % 2", ExpectedResult = 0)]
    [TestCase("Id / 1", ExpectedResult = 10)]
    [TestCase("Id / 2", ExpectedResult = 5)]
    [TestCase("Id / 8", ExpectedResult = 1)]
    // [TestCase("Id / 8.0", ExpectedResult = 1.25)] // This doesn't work yet because we don't have upcasts
    public object Select_Expressions(string expr)
    {
        var result = Query($"SELECT {expr} FROM table where Id = 10;").AsRowList();

        result.Should().HaveCount(1);
        return result[0].Values[0]!;
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
    // Don't pass yet
    // [TestCase("Id > 9900")]
    // [TestCase("Id > 10000 - 100")]
    // [TestCase("100 > Id")]
    // [TestCase("99 >= Id")]
    // [TestCase("9900 > Id")]
    // [TestCase("10000 - 100 < Id")]
    // [TestCase("Id between 0 and 100")]
    public void Where(string expr)
    {
        var result = Query($"SELECT Id FROM table where {expr};").AsRowList();
        result.Select(r => r.Values.Single()).Should().HaveCount(100);
    }
}
