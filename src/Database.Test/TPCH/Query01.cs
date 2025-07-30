using Database.Core;
using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Planner;
using FluentAssertions;

namespace Database.Test.TPCH;

public partial class TPCHTests
{
    [Test]
    public void Q01()
    {
        var query = @"
            select
                l_returnflag,
                l_linestatus,
                count(*) as count_order,
                sum(l_quantity) as sum_qty,
                sum(l_extendedprice) as sum_base_price,

                avg(l_quantity) as avg_qty,
                avg(l_extendedprice) as avg_price,
                avg(l_discount) as avg_disc,
                -- should be 1, but need automatic casts
                sum(l_extendedprice*(1.0-l_discount)*(1.0+l_tax)) as sum_charge,
                sum(l_extendedprice*(1.0-l_discount)) as sum_disc_price
            from lineitem
            where l_shipdate <= date '1998-12-01' - interval '30' day
            group by l_returnflag, l_linestatus
            order by l_returnflag, l_linestatus
        ;";

        var result = Query(query).AsRowList();
        result.Should().HaveCount(4);
    }

    // Null value handling is weird. Either we're losing it in the write from duckdb, or we're parsing it unconditionally?
    // Misc performance ideas
    // Reference local copy of parquet.net
    // Skip decoding unused columns, 1,000ms
    // UnpackNullsTypeFast - can be vectorized. check assembly. 68ms ~ 6% https://github.com/aloneguid/parquet-dotnet/blob/124cd02109aaccf5cbfed08c63c9587a126d7fc2/src/Parquet/Extensions/UntypedArrayExtensions.cs#L1039C25-L1039C44
    // Remove Enumerable.Count inside datacolumn ctor 64ms
    // File format ideas
    // - disable block compression
    // - check the row group size
    // - https://arxiv.org/pdf/2304.05028
    // - see if bloom filters are enabled
    // - verify parquet > 2.9 where PageIndex is used for zone maps
}
