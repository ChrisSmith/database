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
    public void Q13()
    {
        var query = @"
SELECT
    c_count,
    COUNT(*) AS custdist
FROM (
    SELECT
        c_custkey,
        COUNT(o_orderkey)
    FROM
        customer LEFT OUTER JOIN orders ON
            c_custkey = o_custkey
            AND o_comment NOT LIKE '%special%requests%'
    GROUP BY
        c_custkey
) AS c_orders (c_custkey, c_count)
GROUP BY
    c_count
ORDER BY
    custdist DESC,
    c_count DESC;

        ";
        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }
}
