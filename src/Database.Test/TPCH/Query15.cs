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
    public void Q15()
    {
        var query = @"
CREATE VIEW revenue[STREAM_ID] (supplier_no, total_revenue) AS
SELECT
    l_suppkey,
    SUM(l_extendedprice * (1 - l_discount)) as total_revenue
FROM
    lineitem
WHERE
    l_shipdate >= date '[DATE]'
    AND l_shipdate < date '[DATE]' + interval '3' month
GROUP BY
    l_suppkey;

SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM
    supplier,
    revenue[STREAM_ID]
WHERE
    s_suppkey = supplier_no
    AND total_revenue = (
        SELECT
            MAX(total_revenue)
        FROM
            revenue[STREAM_ID]
    )
ORDER BY
    s_suppkey;

DROP VIEW revenue[STREAM_ID];
        ";
        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }
}
