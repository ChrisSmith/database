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
    public void Q22()
    {
        var query = @"
SELECT
    cntrycode,
    count(*) as numcust,
    sum(c_acctbal) as totacctbal
FROM (
    SELECT
        substring(c_phone FROM 1 FOR 2) as cntrycode,
        c_acctbal
    FROM
        customer
    WHERE
        substring(c_phone FROM 1 FOR 2) IN
            ('[I1]','[I2]','[I3]','[I4]','[I5]','[I6]','[I7]')
        AND c_acctbal > (
            SELECT
                avg(c_acctbal)
            FROM
                customer
            WHERE
                c_acctbal > 0.00
                AND substring(c_phone FROM 1 FOR 2) IN
                    ('[I1]','[I2]','[I3]','[I4]','[I5]','[I6]','[I7]')
        )
        AND NOT EXISTS (
            SELECT
                *
            FROM
                orders
            WHERE
                o_custkey = c_custkey
        )
) AS custsale
GROUP BY
    cntrycode
ORDER BY
    cntrycode;
        ";
        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }
}
