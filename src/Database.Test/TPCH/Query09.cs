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
    public void Q09()
    {
        var query = @"
            SELECT
                nation,
                o_year,
                sum(amount) as sum_profit
            FROM (
                SELECT
                    n_name as nation,
                    extract(year from o_orderdate) as o_year,
                    l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
                FROM
                    part,
                    supplier,
                    lineitem,
                    partsupp,
                    orders,
                    nation
                WHERE
                    s_suppkey = l_suppkey
                    AND ps_suppkey = l_suppkey
                    AND ps_partkey = l_partkey
                    AND p_partkey = l_partkey
                    AND o_orderkey = l_orderkey
                    AND s_nationkey = n_nationkey
                    AND p_name LIKE '%[COLOR]%'
            ) AS profit
            GROUP BY
                nation,
                o_year
            ORDER BY
                nation,
                o_year DESC;

        ;";
        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }
}
