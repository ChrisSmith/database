using BenchmarkDotNet.Running;

namespace MyBenchmarks
{
    public class Program
    {
        public static void Main(string[] args)
        {
            // var c = new MultiplyAdd();
            // c.Benchmark_Jit_MultiplyAdd_Fused();

            // var baseline = c.Benchmark_Multiply_Then_Add_Loop();
            // var fused = c.Benchmark_MultiplyAdd_Fused();
            // var fusedVector = c.Benchmark_MultiplyAdd_Fused_Vector();
            // var jitFused = c.Benchmark_Jit_MultiplyAdd_Fused();
            //
            // const double TOLERANCE = 1e-6;
            // Console.WriteLine($"{Math.Abs(baseline - fused) < TOLERANCE} {baseline} == {fused}");
            // Console.WriteLine($"{Math.Abs(baseline - fusedVector) < TOLERANCE} {baseline} == {fusedVector}");
            // Console.WriteLine($"{Math.Abs(baseline - jitFused) < TOLERANCE} {baseline} == {jitFused}");
            //
            // double d=0;
            // for (var i = 0; i < 1000; i++)
            // {
            //     d += c.Benchmark_Jit_MultiplyAdd_Fused();
            // }
            // Console.WriteLine(d);

            var summary = BenchmarkRunner.Run<DbDecimalBenchmark>();
        }
    }
}
