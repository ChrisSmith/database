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
    public void Q01()
    {
        var query = File.ReadAllText("TPCH/Queries/query_01.sql");

        var result = Query(query).AsRowList();
        result.Should().HaveCount(4);

        result.Should().BeEquivalentTo(new List<Row>
        {
            new(["A", "F", 1478493, 37734107.00m, 56586554400.73m, 25.522005853257337, 38273.129734621674, 0.049985295838397614, 55909065222.827692m, 53758257134.8700m]),
            new(["N", "F", 38854, 991417.00m, 1487504710.38m, 25.516471920522985, 38284.4677608483, 0.0500934266742163, 1469649223.194375m, 1413082168.0541m]),
            new(["N", "O", 2995314, 76385881.00m, 114563004757.36m, 25.501794135773412, 38247.410707979194, 0.05000143223715443, 113193138614.045709m, 108835868867.4998m]),
            new(["R", "F", 1478870, 37719753.00m, 56568041380.90m, 25.50579361269077, 38250.85462609966, 0.05000940583012706, 55889619119.831932m, 53741292684.6040m])
        });
    }

    // Null value handling is weird. Either we're losing it in the write from duckdb, or we're parsing it unconditionally?
    // Misc performance ideas
    // Reference local copy of parquet.net
    // Skip decoding unused columns, 1,000ms
    // UnpackNullsTypeFast - can be vectorized. check assembly. 68ms ~ 6% https://github.com/aloneguid/parquet-dotnet/blob/124cd02109aaccf5cbfed08c63c9587a126d7fc2/src/Parquet/Extensions/UntypedArrayExtensions.cs#L1039C25-L1039C44
    // Remove Enumerable.Count inside datacolumn ctor 64ms
    // File format ideas
    // - disable block compression
    // - check the row group size
    // - https://arxiv.org/pdf/2304.05028
    // - see if bloom filters are enabled
    // - verify parquet > 2.9 where PageIndex is used for zone maps
}
