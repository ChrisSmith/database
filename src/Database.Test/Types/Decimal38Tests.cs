using Database.Core.Types;
using FluentAssertions;

namespace Database.Test.Types;

public class Decimal38Tests
{
    private const int size = 10_000;
    private readonly decimal[] data;
    private readonly Decimal38[] dbdata;

    private const long max = 9_999_999_999_999;

    public Decimal38Tests()
    {
        var rand = new Random(219038723);
        data = new decimal[size];
        dbdata = new Decimal38[size];
        for (var i = 0; i < data.Length; i++)
        {
            var v = rand.NextInt64(-max, max);
            var s = rand.NextInt64(0, 99);
            data[i] = decimal.Parse($"{v}.{(int)(s / 100)}");
            dbdata[i] = new Decimal38(data[i]);
        }
    }

    public static object[][] AddTestCases => new object[][]
    {
        [0m, 0m, 0, 0m],
        [1m, 0m, 1000000, 1m],
        [0m, 1m, 1000000, 1m],
        [100.01m, 0.99m, 101000000, 101m],
        [100.51m, 0.50m, 101010000, 101.01m],
    };

    [TestCaseSource(nameof(AddTestCases))]
    public void Add(decimal left, decimal right, int scaled, decimal expected)
    {
        Decimal38 l = left;
        Decimal38 r = right;
        Decimal38 res = l + r;
        res.Value.Should().Be(scaled);
        res.AsDecimal().Should().Be(expected);
    }

    public static object[][] MultiplyTestCases => new object[][]
    {
        [0m, 0m, 0, 0m],
        [1m, 0m, 0, 0m],
        [0m, 1m, 0, 0m],
        [1m, 1m, 1000000, 1m],
        [1m, 5m, 5000000, 5m],
        [3m, 5m, 15000000, 15m],
        [100.01m, 0.99m, 99009900, 99.0099m],
        [100.51m, 0.50m, 50255000, 50.255m],
    };

    [TestCaseSource(nameof(MultiplyTestCases))]
    public void Multiply(decimal left, decimal right, int scaled, decimal expected)
    {
        Decimal38 l = left;
        Decimal38 r = right;
        Decimal38 res = l * r;
        res.Value.Should().Be((ulong)scaled);
        res.AsDecimal().Should().Be(expected);
    }

    public static object[][] DivideTestCases => new object[][]
    {
        [0m, 1m, 0, 0m],
        [1m, 1m, 1000000, 1m],
        [1m, 5m, 200000, 0.2m],
        [1m, 3m, 333333, 0.333333m],
        [100.01m, 0.99m, 101020202, 101.020202m],
        [100.51m, 0.50m, 201020000, 201.02m],
    };

    [TestCaseSource(nameof(DivideTestCases))]
    public void Divide(decimal left, decimal right, int scaled, decimal expected)
    {
        Decimal38 l = left;
        Decimal38 r = right;
        Decimal38 res = l / r;
        res.Value.Should().Be(scaled);
        res.AsDecimal().Should().Be(expected);
    }

    [Test]
    public void Add_Random()
    {
        var failureSample = new List<(int, int, decimal, Decimal38)>(10);
        var matches = 0;
        var failures = 0;
        for (var i = 0; i < data.Length; i++)
        {
            for (var j = 0; j < data.Length; j++)
            {
                var expected = data[i] + data[j];
                var actual = dbdata[i] + dbdata[j];
                var ok = actual == new Decimal38(expected);
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
