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
    public void Q02()
    {
        var query = @"
            select
                s_acctbal,
                s_name,
                n_name,
                p_partkey,
                p_mfgr,
                s_address,
                s_phone,
                s_comment
            from
                part,
                supplier,
                partsupp,
                nation,
                region
            where
                    p_partkey = ps_partkey
                and s_suppkey = ps_suppkey
                and p_size = 15
                and p_type like '%BRASS'
                and s_nationkey = n_nationkey
                and n_regionkey = r_regionkey
                and r_name = 'EUROPE'
                and ps_supplycost = (
                    select
                        min(ps_supplycost)
                        from partsupp, supplier, nation, region
                        where
                            p_partkey = ps_partkey
                        and s_suppkey = ps_suppkey
                        and s_nationkey = n_nationkey
                        and n_regionkey = r_regionkey
                        and r_name = 'EUROPE'
                    )
            order by
                s_acctbal desc,
                n_name,
                s_name,
                p_partkey
            limit 100;
        ";

        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }
}
