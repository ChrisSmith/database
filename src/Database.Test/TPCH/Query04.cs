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
    public void Q05()
    {
        var query = @"
            select
                o_orderpriority,
                count(*) as order_count
            from orders
            where
                    o_orderdate >= date '1993-07-01'
                and o_orderdate < date '1993-07-01' + interval '3' month
                and exists (
                    select *
                    from
                    where
                    lineitem
                    l_orderkey = o_orderkey
                    and l_commitdate < l_receiptdate
                )
            group by
                o_orderpriority
            order by
                o_orderpriority;
        ";

        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }
}
