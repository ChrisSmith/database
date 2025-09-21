using Database.Core.Types;
using FluentAssertions;

namespace Database.Test.Types;

public class Decimal15Tests
{
    private const int size = 10_000;
    private readonly decimal[] data;
    private readonly Decimal15[] dbdata;

    private const long max = 9_999_999_999_999;

    public Decimal15Tests()
    {
        var rand = new Random(219038723);
        data = new decimal[size];
        dbdata = new Decimal15[size];
        for (var i = 0; i < data.Length; i++)
        {
            var v = rand.NextInt64(-max, max);
            var s = rand.NextInt64(0, 99);
            data[i] = decimal.Parse($"{v}.{(int)(s / 100)}");
            dbdata[i] = new Decimal15(data[i]);
        }
    }

    public static object[][] AddTestCases => new object[][]
    {
        [0m, 0m, 0, 0m],
        [1m, 0m, 100, 1m],
        [0m, 1m, 100, 1m],
        [100.01m, 0.99m, 10100, 101m],
        [100.51m, 0.50m, 10101, 101.01m],
    };

    [TestCaseSource(nameof(AddTestCases))]
    public void Add(decimal left, decimal right, int scaled, decimal expected)
    {
        Decimal15 l = left;
        Decimal15 r = right;
        Decimal15 res = l + r;
        res.Value.Should().Be(scaled);
        res.AsDecimal().Should().Be(expected);
    }

    public static object[][] MultiplyTestCases => new object[][]
    {
        [0m, 0m, 0, 0m],
        [1m, 0m, 0, 0m],
        [0m, 1m, 0, 0m],
        [1m, 1m, 100, 1m],
        [1m, 5m, 500, 5m],
        [3m, 5m, 1500, 15m],
        [100.01m, 0.99m, 9900, 99.00m], // currently we truncate instead of rounding
        [100.51m, 0.50m, 5025, 50.25m],
    };

    [TestCaseSource(nameof(MultiplyTestCases))]
    public void Multiply(decimal left, decimal right, int scaled, decimal expected)
    {
        Decimal15 l = left;
        Decimal15 r = right;
        Decimal15 res = l * r;
        res.Value.Should().Be(scaled);
        res.AsDecimal().Should().Be(expected);
    }

    public static object[][] DivideTestCases => new object[][]
    {
        [0m, 1m, 0, 0m],
        [1m, 1m, 100, 1m],
        [1m, 5m, 20, 0.2m],
        [1m, 3m, 33, 0.33m],
        [100.01m, 0.99m, 10102, 101.02m],
        [100.51m, 0.50m, 20102, 201.02m],
    };

    [TestCaseSource(nameof(DivideTestCases))]
    public void Divide(decimal left, decimal right, int scaled, decimal expected)
    {
        Decimal15 l = left;
        Decimal15 r = right;
        Decimal15 res = l / r;
        res.Value.Should().Be(scaled);
        res.AsDecimal().Should().Be(expected);
    }

    [Test]
    public void Add_Random()
    {
        var failureSample = new List<(int, int, decimal, Decimal15)>(10);
        var matches = 0;
        var failures = 0;
        for (var i = 0; i < data.Length; i++)
        {
            for (var j = 0; j < data.Length; j++)
            {
                var expected = data[i] + data[j];
                var actual = dbdata[i] + dbdata[j];
                var ok = actual == new Decimal15(expected);
                if (ok)
                {
                    matches++;
                }
                else
                {
                    failures++;
                    if (failureSample.Count < 10)
                    {
                        failureSample.Add((i, j, expected, actual));
                    }
                }
            }
        }

        if (failureSample.Any())
        {
            failureSample.Should().BeEmpty($"got {failures} failures and {matches} matches");
        }
    }

}
