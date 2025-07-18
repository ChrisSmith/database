using Database.Core;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Planner;
using FluentAssertions;

namespace Database.Test.TPCH;

public class Query01
{
    private Catalog _catalog;

    [OneTimeSetUp]
    public void OneTimeSetup()
    {
        var homeDir = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        var dataPath = Path.Combine(homeDir, "src/database/tpch/1");
        _catalog = new Catalog();

        var tableName = "lineitem";
        var colId = 0;
        _catalog.Tables.Add(new TableSchema((TableId)1, tableName, new List<ColumnSchema>
        {
            new((ColumnId)colId++, "l_orderkey", DataType.Long, typeof(long)),
            new((ColumnId)colId++, "l_partkey", DataType.Int, typeof(int)),
            new((ColumnId)colId++, "l_suppkey", DataType.Int, typeof(int)),
            new((ColumnId)colId++, "l_linenumber", DataType.Int, typeof(int)),
            new((ColumnId)colId++, "l_quantity", DataType.Double, typeof(double)),
            new((ColumnId)colId++, "l_extendedprice", DataType.Double, typeof(double)),
            new((ColumnId)colId++, "l_discount", DataType.Double, typeof(double)),
            new((ColumnId)colId++, "l_tax", DataType.Double, typeof(double)),
            new((ColumnId)colId++, "l_returnflag", DataType.String, typeof(string)),
            new((ColumnId)colId++, "l_linestatus", DataType.String, typeof(string)),
            new((ColumnId)colId++, "l_shipdate", DataType.Date, typeof(DateTime)),
            new((ColumnId)colId++, "l_commitdate", DataType.Date, typeof(DateTime)),
            new((ColumnId)colId++, "l_receiptdate", DataType.Date, typeof(DateTime)),
            new((ColumnId)colId++, "l_shipinstruct", DataType.String, typeof(string)),
            new((ColumnId)colId++, "l_shipmode", DataType.String, typeof(string)),
            new((ColumnId)colId++, "l_comment", DataType.String, typeof(string)),
        }, Path.Join(dataPath, $"{tableName}.parquet")));
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
    public void Runs()
    {
        var query = @"
            select
                sum(l_quantity) as sum_qty,
                sum(l_extendedprice) as sum_base_price,

                avg(l_quantity) as avg_qty,
                avg(l_extendedprice) as avg_price,
                avg(l_discount) as avg_disc
            from lineitem
        ;";
        // TODO support *
        // count(*) as count_order
        // TODO support groupings
        // sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
        // sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
        // TODO support comments
        // l_returnflag,
        // l_linestatus,

        // --            where l_shipdate <= date '1998-12-01' - interval '[DELTA]' day (3)
        //     --            group by l_returnflag, l_linestatus
        // --            order by l_returnflag, l_linestatus;

        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }
}
