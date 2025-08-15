using Database.Core.Execution;
using FluentAssertions;

namespace Database.Test.TPCH;

public partial class TPCHTests
{
    [Test]
    public void Q01()
    {
        var query = ReadQuery("query_01.sql");

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

    [Test]
    public void Q02()
    {
        var query = ReadQuery("query_02.sql");

        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }

    [Test]
    public void Q03()
    {
        var query = ReadQuery("query_03.sql");

        var result = Query(query).AsRowList();
        result.Should().BeEquivalentTo(new List<Row>
        {
            new ([2456423, 406181.0111m, new DateTime(1995, 03, 05), 0]),
            new ([3459808, 405838.6989m, new DateTime(1995, 03, 04), 0]),
            new ([492164, 390324.0610m, new DateTime(1995, 02, 19), 0]),
            new ([1188320, 384537.9359m, new DateTime(1995, 03, 09), 0]),
            new ([2435712, 378673.0558m, new DateTime(1995, 02, 26), 0]),
            new ([4878020, 378376.7952m, new DateTime(1995, 03, 12), 0]),
            new ([5521732, 375153.9215m, new DateTime(1995, 03, 13), 0]),
            new ([2628192, 373133.3094m, new DateTime(1995, 02, 22), 0]),
            new ([993600, 371407.4595m, new DateTime(1995, 03, 05), 0]),
            new ([2300070, 367371.1452m, new DateTime(1995, 03, 13), 0]),

        });
    }

    [Test]
    public void Q04()
    {
        var query = ReadQuery("query_04.sql");

        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }

    [Test]
    public void Q05()
    {
        var query = ReadQuery("query_05.sql");

        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }

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

    [Test]
    public void Q07()
    {
        var query = ReadQuery("query_07.sql");
        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }

    [Test]
    public void Q08()
    {
        var query = ReadQuery("query_08.sql");
        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }

    [Test]
    public void Q09()
    {
        var query = ReadQuery("query_09.sql");
        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }

    [Test]
    public void Q10()
    {
        var query = ReadQuery("query_10.sql");
        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }

    [Test]
    public void Q11()
    {
        var query = ReadQuery("query_11.sql");
        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }

    [Test]
    public void Q12()
    {
        var query = ReadQuery("query_12.sql");
        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }

    [Test]
    public void Q13()
    {
        var query = ReadQuery("query_13.sql");
        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }

    [Test]
    public void Q14()
    {
        var query = ReadQuery("query_14.sql");
        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }

    [Test]
    public void Q15()
    {
        var query = ReadQuery("query_15.sql");
        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }

    [Test]
    public void Q16()
    {
        var query = ReadQuery("query_16.sql");
        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }

    [Test]
    public void Q17()
    {
        var query = ReadQuery("query_17.sql");
        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }

    [Test]
    public void Q18()
    {
        var query = ReadQuery("query_18.sql");
        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }

    [Test]
    public void Q19()
    {
        var query = ReadQuery("query_19.sql");
        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }

    [Test]
    public void Q20()
    {
        var query = ReadQuery("query_20.sql");
        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }

    [Test]
    public void Q21()
    {
        var query = ReadQuery("query_21.sql");
        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }

    [Test]
    public void Q22()
    {
        var query = ReadQuery("query_22.sql");
        var result = Query(query).AsRowList();
        result.Should().HaveCountGreaterOrEqualTo(1);
    }
}
