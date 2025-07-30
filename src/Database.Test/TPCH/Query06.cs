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
                    l_shipdate >= date '1994-01-01'
                and l_shipdate < date '1994-01-01' + interval '1' year
                and l_discount between 0.06 - 0.01 and 0.06 + 0.01
                and l_quantity < 24;
        ";
        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }
}
