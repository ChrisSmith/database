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
    public void Q06()
    {
        var query = @"
            select
               sum(l_extendedprice*l_discount) as revenue
            from
                lineitem
            where
                    l_shipdate >= date '[DATE]'
                and l_shipdate < date '[DATE]' + interval '1' year
                and l_discount between [DISCOUNT] - 0.01 and [DISCOUNT] + 0.01
                and l_quantity < [QUANTITY];
        ;";
        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }
}
