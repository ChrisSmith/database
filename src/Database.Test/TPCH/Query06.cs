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
        var query = ReadQuery("query_06.sql");
        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
        result.Should().BeEquivalentTo(new List<Row>
        {
            new([123141078.2283m])
        });
    }
}
