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
        _catalog = new Catalog();
        TestDatasets.AddTestDatasetsToCatalog(_catalog);
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
                count(*) as count_order,
                sum(l_quantity) as sum_qty,
                sum(l_extendedprice) as sum_base_price,

                avg(l_quantity) as avg_qty,
                avg(l_extendedprice) as avg_price,
                avg(l_discount) as avg_disc
            from lineitem
        ;";
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

    // Misc performance ideas
    // Reference local copy of parquet.net
    // Skip decoding unused columns, 1,000ms
    // UnpackNullsTypeFast - can be vectorized. check assembly. 68ms ~ 6% https://github.com/aloneguid/parquet-dotnet/blob/124cd02109aaccf5cbfed08c63c9587a126d7fc2/src/Parquet/Extensions/UntypedArrayExtensions.cs#L1039C25-L1039C44
    // Remove Enumerable.Count inside datacolumn ctor 64ms
}
