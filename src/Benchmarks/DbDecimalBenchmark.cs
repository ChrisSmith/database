using BenchmarkDotNet.Attributes;
using Database.Core.Types;

namespace MyBenchmarks;

[SimpleJob(iterationCount: 20)]
public class DbDecimalBenchmark
{
    private const int size = 10_000;
    private readonly decimal[] data;
    private readonly Decimal15[] dbdata;

    public DbDecimalBenchmark()
    {
        data = new decimal[size];
        dbdata = new Decimal15[size];
        for (var i = 0; i < data.Length; i++)
        {
            var v = Random.Shared.NextInt64(0, int.MaxValue);
            var s = Random.Shared.NextInt64(0, 99);
            data[i] = new decimal(v) + new decimal(s) / 100;
            dbdata[i] = new Decimal15(v * 100 + s);
        }
    }

    [Benchmark(Baseline = true)]
    public decimal Decimal()
    {
        decimal result = 0;
        for (var i = 0; i < data.Length; i++)
        {
            result += data[i];
        }
        return result;
    }

    [Benchmark]
    public Decimal15 DbDecimal()
    {
        var result = new Decimal15(0);
        for (var i = 0; i < data.Length; i++)
        {
            result += dbdata[i];
        }
        return result;
    }
}
