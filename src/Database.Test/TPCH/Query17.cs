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
    public void Q17()
    {
        var query = @"
SELECT
    sum(l_extendedprice) / 7.0 AS avg_yearly
FROM
    lineitem,
    part
WHERE
    p_partkey = l_partkey
    AND p_brand = 'Brand#23'
    AND p_container = 'MED BOX'
    AND l_quantity < (
        SELECT
            0.2 * avg(l_quantity)
        FROM
            lineitem
        WHERE
            l_partkey = p_partkey
    );
        ";
        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }
}
