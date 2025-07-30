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
    public void Q03()
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
            limit 10
            ;
        ";

        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }
}
