using Database.BenchmarkRunner;
using Database.Core.Execution;
using FluentAssertions;

namespace Database.Test.TPCH;

[CancelAfter(90_000)]
public class TPCHTests
{
    private static DatabaseRunner _runner;
    private string ReadQuery(string name) => TPCHHelpers.ReadQuery(name);

    [OneTimeSetUp]
    public static void Setup()
    {
        _runner = new DatabaseRunner();
        _runner.Initialize();
    }

    [Test]
    public void Q01(CancellationToken token)
    {
        var query = ReadQuery("query_01.sql");

        var result = _runner.Run(query, token);
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
    public void Q02(CancellationToken token)
    {
        var query = ReadQuery("query_02.sql");

        var result = _runner.Run(query, token);
        result.Should().HaveCountGreaterOrEqualTo(1);
    }

    [Test]
    public void Q03(CancellationToken token)
    {
        var query = ReadQuery("query_03.sql");

        var result = _runner.Run(query, token);
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
    public void Q04(CancellationToken token)
    {
        var query = ReadQuery("query_04.sql");

        var result = _runner.Run(query, token); ;
        result.Should().HaveCountGreaterOrEqualTo(1);
    }

    [Test]
    public void Q05(CancellationToken token)
    {
        var query = ReadQuery("query_05.sql");

        var result = _runner.Run(query, token); ;
        result.Should().BeEquivalentTo(new List<Row>
        {
            new (["INDONESIA" , 55502041.1697m]),
            new (["VIETNAM"   , 55295086.9967m]),
            new (["CHINA"     , 53724494.2566m]),
            new (["INDIA"     , 52035512.0002m]),
            new (["JAPAN"     , 45410175.6954m]),
        });
    }

    [Test]
    public void Q06(CancellationToken token)
    {
        var query = ReadQuery("query_06.sql");
        var result = _runner.Run(query, token); ;
        result.Should().HaveCountGreaterOrEqualTo(1);
        result.Should().BeEquivalentTo(new List<Row>
        {
            new([123141078.2283m])
        });
    }

    [Test]
    public void Q07(CancellationToken token)
    {
        var query = ReadQuery("query_07.sql");
        var result = _runner.Run(query, token); ;
        result.Should().BeEquivalentTo(new List<Row>
        {
            new(["FRANCE", "GERMANY", 1995, 54639732.7336m]),
            new(["FRANCE", "GERMANY", 1996, 54633083.3076m]),
            new(["GERMANY", "FRANCE", 1995, 52531746.6697m]),
            new(["GERMANY", "FRANCE", 1996, 52520549.0224m]),
        });
    }

    [Test]
    public void Q08(CancellationToken token)
    {
        var query = ReadQuery("query_08.sql");
        var result = _runner.Run(query, token); ;
        result.Should().BeEquivalentTo(new List<Row>
        {
            new ([1995, 0.0344358904066547974259817099M]),
            new ([1996, 0.0414855212935303207474250901M]),
        });
    }

    [Test]
    public void Q09(CancellationToken token)
    {
        var query = ReadQuery("query_09.sql");
        var result = _runner.Run(query, token); ;
        result.Should().HaveCount(175);
        result[0..20].Should().BeEquivalentTo(new List<Row>
        {
            new (["ALGERIA", 1998, 27136900.1803m]),
            new (["ALGERIA", 1997, 48611833.4962m]),
            new (["ALGERIA", 1996, 48285482.6782m]),
            new (["ALGERIA", 1995, 44402273.5999m]),
            new (["ALGERIA", 1994, 48694008.0668m]),
            new (["ALGERIA", 1993, 46044207.7838m]),
            new (["ALGERIA", 1992, 45636849.4881m]),
            new (["ARGENTINA", 1998, 28341663.7848m]),
            new (["ARGENTINA", 1997, 47143964.1176m]),
            new (["ARGENTINA", 1996, 45255278.6021m]),
            new (["ARGENTINA", 1995, 45631769.2054m]),
            new (["ARGENTINA", 1994, 48268856.3547m]),
            new (["ARGENTINA", 1993, 48605593.6162m]),
            new (["ARGENTINA", 1992, 46654240.7487m]),
            new (["BRAZIL", 1998, 26527736.3960m]),
            new (["BRAZIL", 1997, 45640660.7677m]),
            new (["BRAZIL", 1996, 45090647.1630m]),
            new (["BRAZIL", 1995, 44015888.5132m]),
            new (["BRAZIL", 1994, 44854218.8932m]),
            new (["BRAZIL", 1993, 45766603.7379m]),
        });
    }

    [Test]
    public void Q10(CancellationToken token)
    {
        var query = ReadQuery("query_10.sql");
        var result = _runner.Run(query, token); ;
        // result.Should().HaveCount(37967);
        result[0..10].Should().BeEquivalentTo(new List<Row>
        {
            new ([57040, "Customer#000057040", 734235.2455m, 632.87m, "JAPAN", "nICtsILWBB", "22-895-641-3466", "ep. blithely regular foxes promise slyly furiously ironic depend"]),
            new ([143347, "Customer#000143347", 721002.6948m, 2557.47m, "EGYPT", ",Q9Ml3w0gvX", "14-742-935-3718", "endencies sleep. slyly express deposits nag carefully around the even tithes. slyly regular "]),
            new ([60838, "Customer#000060838", 679127.3077m, 2454.77m, "BRAZIL","VWmQhWweqj5hFpcvhGFBeOY9hJ4m", "12-913-494-9813","tes. final instructions nag quickly according to"]),
            new ([101998, "Customer#000101998", 637029.5667m, 3790.89m, "UNITED KINGDOM", "0,ORojfDdyMca2E2H", "33-593-865-6378","ost carefully. slyly regular packages cajole about the blithely final ideas. permanently daring deposit"]),
            new ([125341, "Customer#000125341", 633508.0860m, 4983.51m, "GERMANY", "9YRcnoUPOM7Sa8xymhsDHdQg", "17-582-695-5962","ly furiously brave packages. quickly regular dugouts kindle furiously carefully bold theodolites. "]),
            new ([25501, "Customer#000025501", 620269.7849m, 7725.04m, "ETHIOPIA", "sr4VVVe3xCJQ2oo2QEhi19D,pXqo6kOGaSn2", "15-874-808-6793","y ironic foxes hinder according to the furiously permanent dolphins. pending ideas integrate blithely from "]),
            new ([115831, "Customer#000115831", 596423.8672m, 5098.10m, "FRANCE","AlMpPnmtGrOFrDMUs5VLo EIA,Cg,Rw5TBuBoKiO", "16-715-386-3788","unts nag carefully final packages. express theodolites are regular ac"]),
            new ([84223, "Customer#000084223", 594998.0239m, 528.65m, "UNITED KINGDOM", "Eq51o UpQ4RBr  fYTdrZApDsPV4pQyuPq", "33-442-824-8191","longside of the slyly final deposits. blithely final platelets about the blithely i"]),
            new ([54289, "Customer#000054289", 585603.3918m, 5583.02m, "IRAN",  "x3ouCpz6,pRNVhajr0CCQG1", "20-834-292-4707"," cajole furiously after the quickly unusual fo"]),
            new ([39922, "Customer#000039922", 584878.1134m, 7321.11m, "GERMANY",  "2KtWzW,FYkhdWBfobp6SFXWYKjvU9", "17-147-757-8036","ironic deposits sublate furiously. carefully regular theodolites along the b"]),
        });
    }

    [Test]
    public void Q11(CancellationToken token)
    {
        var query = ReadQuery("query_11.sql");
        var result = _runner.Run(query, token); ;
        result.Should().HaveCount(1048);
        result[..20].Should().BeEquivalentTo(new List<Row>
        {
            new([129760, 17538456.86m]),
            new([166726, 16503353.92m]),
            new([191287, 16474801.97m]),
            new([161758, 16101755.54m]),
            new([34452, 15983844.72m]),
            new([139035, 15907078.34m]),
            new([9403, 15451755.62m]),
            new([154358, 15212937.88m]),
            new([38823, 15064802.86m]),
            new([85606, 15053957.15m]),
            new([33354, 14408297.40m]),
            new([154747, 14407580.68m]),
            new([82865, 14235489.78m]),
            new([76094, 14094247.04m]),
            new([222, 13937777.74m]),
            new([121271, 13908336.00m]),
            new([55221, 13716120.47m]),
            new([22819, 13666434.28m]),
            new([76281, 13646853.68m]),
            new([85298, 13581154.93m]),
        });
    }

    [Test]
    public void Q12(CancellationToken token)
    {
        var query = ReadQuery("query_12.sql");
        var result = _runner.Run(query, token); ;
        result.Should().BeEquivalentTo(new List<Row>
        {
            new (["MAIL", 6202, 9324]),
            new (["SHIP", 6200, 9262]),
        });
    }

    [Test]
    public void Q13(CancellationToken token)
    {
        var query = ReadQuery("query_13.sql");
        var result = _runner.Run(query, token); ;
        result.Should().BeEquivalentTo(new List<Row>
        {
            new([0, 50004]),
            new([10, 6668]),
            new([9, 6563]),
            new([11, 6004]),
            new([8, 5890]),
            new([12, 5600]),
            new([13, 5029]),
            new([19, 4805]),
            new([7, 4680]),
            new([18, 4531]),
            new([20, 4507]),
            new([14, 4473]),
            new([15, 4463]),
            new([17, 4445]),
            new([16, 4410]),
            new([21, 4168]),
            new([22, 3742]),
            new([6, 3273]),
            new([23, 3189]),
            new([24, 2700]),
            new([25, 2090]),
            new([5, 1957]),
            new([26, 1653]),
            new([27, 1177]),
            new([4, 1010]),
            new([28, 901]),
            new([29, 564]),
            new([3, 408]),
            new([30, 378]),
            new([31, 242]),
            new([32, 133]),
            new([2, 128]),
            new([33, 72]),
            new([34, 52]),
            new([35, 32]),
            new([36, 20]),
            new([1, 20]),
            new([37, 8]),
            new([38, 4]),
            new([41, 3]),
            new([40, 3]),
            new([39, 1]),
        });
    }

    [Test]
    public void Q14(CancellationToken token)
    {
        var query = ReadQuery("query_14.sql");
        var result = _runner.Run(query, token); ;
        result.Should().BeEquivalentTo(new List<Row>
        {
            new([16.380778626395540147992741460m]),
        });
    }

    [Test]
    public void Q15(CancellationToken token)
    {
        var query = ReadQuery("query_15.sql");
        var result = _runner.Run(query, token); ;
        result.Should().HaveCountGreaterOrEqualTo(1);
    }

    [Test]
    public void Q16(CancellationToken token)
    {
        var query = ReadQuery("query_16.sql");
        var result = _runner.Run(query, token); ;
        result.Should().HaveCountGreaterOrEqualTo(1);
    }

    [Test]
    public void Q17(CancellationToken token)
    {
        var query = ReadQuery("query_17.sql");
        var result = _runner.Run(query, token); ;
        result.Should().BeEquivalentTo(new List<Row>
        {
            new ([348406.05428571428571428571429m]),
        });
    }

    [Test]
    public void Q18(CancellationToken token)
    {
        var query = ReadQuery("query_18.sql");
        var result = _runner.Run(query, token); ;
        result.Should().HaveCount(57);
        result[0..20].Should().BeEquivalentTo(new List<Row>
        {
            new(["Customer#000128120", 128120L, 4722021L, new DateTime(1994, 04, 07), 544089.09m, 323.00m]),
            new(["Customer#000144617", 144617L, 3043270L, new DateTime(1997, 02, 12), 530604.44m, 317.00m]),
            new(["Customer#000013940", 13940L, 2232932L, new DateTime(1997, 04, 13), 522720.61m, 304.00m]),
            new(["Customer#000066790", 66790L, 2199712L, new DateTime(1996, 09, 30), 515531.82m, 327.00m]),
            new(["Customer#000046435", 46435L, 4745607L, new DateTime(1997, 07, 03), 508047.99m, 309.00m]),
            new(["Customer#000015272", 15272L, 3883783L, new DateTime(1993, 07, 28), 500241.33m, 302.00m]),
            new(["Customer#000146608", 146608L, 3342468L, new DateTime(1994, 06, 12), 499794.58m, 303.00m]),
            new(["Customer#000096103", 96103L, 5984582L, new DateTime(1992, 03, 16), 494398.79m, 312.00m]),
            new(["Customer#000024341", 24341L, 1474818L, new DateTime(1992, 11, 15), 491348.26m, 302.00m]),
            new(["Customer#000137446", 137446L, 5489475L, new DateTime(1997, 05, 23), 487763.25m, 311.00m]),
            new(["Customer#000107590", 107590L, 4267751L, new DateTime(1994, 11, 04), 485141.38m, 301.00m]),
            new(["Customer#000050008", 50008L, 2366755L, new DateTime(1996, 12, 09), 483891.26m, 302.00m]),
            new(["Customer#000015619", 15619L, 3767271L, new DateTime(1996, 08, 07), 480083.96m, 318.00m]),
            new(["Customer#000077260", 77260L, 1436544L, new DateTime(1992, 09, 12), 479499.43m, 307.00m]),
            new(["Customer#000109379", 109379L, 5746311L, new DateTime(1996, 10, 10), 478064.11m, 302.00m]),
            new(["Customer#000054602", 54602L, 5832321L, new DateTime(1997, 02, 09), 471220.08m, 307.00m]),
            new(["Customer#000105995", 105995L, 2096705L, new DateTime(1994, 07, 03), 469692.58m, 307.00m]),
            new(["Customer#000148885", 148885L, 2942469L, new DateTime(1992, 05, 31), 469630.44m, 313.00m]),
            new(["Customer#000114586", 114586L, 551136L, new DateTime(1993, 05, 19), 469605.59m, 308.00m]),
            new(["Customer#000105260", 105260L, 5296167L, new DateTime(1996, 09, 06), 469360.57m, 303.00m]),
        });
    }

    [Test]
    public void Q19(CancellationToken token)
    {
        var query = ReadQuery("query_19.sql");
        var result = _runner.Run(query, token); ;
        result.Should().HaveCountGreaterOrEqualTo(1);
    }

    [Test]
    public void Q20(CancellationToken token)
    {
        var query = ReadQuery("query_20.sql");
        var result = _runner.Run(query, token); ;
        result.Should().HaveCountGreaterOrEqualTo(1);
    }

    [Test]
    public void Q21(CancellationToken token)
    {
        var query = ReadQuery("query_21.sql");
        var result = _runner.Run(query, token); ;
        result.Should().HaveCountGreaterOrEqualTo(1);
    }

    [Test]
    public void Q22(CancellationToken token)
    {
        var query = ReadQuery("query_22.sql");
        var result = _runner.Run(query, token); ;
        result.Should().HaveCountGreaterOrEqualTo(1);
    }
}
