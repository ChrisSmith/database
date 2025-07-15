using System.Numerics;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;

namespace MyBenchmarks
{
    public class MultiplyAdd
    {
        private const int N = 1000 * 1000;
        private readonly double[] data;

        public MultiplyAdd()
        {
            data = new double[N];
            var rand = new Random(42);
            for (var i = 0; i < N; i++)
            {
                data[i] = rand.NextDouble();
            }
        }

        [Benchmark]
        public double Multiply_Then_Add_Loop()
        {
            var mult = Multiply();

            double res = 0;
            for (var i = 0; i < N; i++)
            {
                res += mult[i];
            }
            return res;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private double[] Multiply()
        {
            double[] buff = new double[N];
            for (var i = 0; i < N; i++)
            {
                buff[i] = data[i] * data[i];
            }
            return buff;
        }

        [Benchmark]
        public double MultiplyAdd_Fused()
        {
            double res = 0;
            for (var i = 0; i < N; i++)
            {
                res += data[i] * data[i];
            }
            return res;
        }

        [Benchmark]
        public double MultiplyAdd_Fused_Vector()
        {
            var acc = new Vector<double>();
            for (var i = 0; i < N; i += Vector<double>.Count)
            {
                var vec = new Vector<double>(data, i);

                acc = Vector.FusedMultiplyAdd(vec, vec, acc);
            }
            return Vector.Sum(acc);
        }
    }

    public class Program
    {
        public static void Main(string[] args)
        {
            // var c = new MultiplyAdd();
            // var baseline = c.Multiply_Then_Add_Loop();
            // var fused = c.MultiplyAdd_Fused();
            // var fusedVector = c.MultiplyAdd_Fused_Vector();
            // const double TOLERANCE = 1e-6;
            // Console.WriteLine($"{Math.Abs(baseline - fused) < TOLERANCE} {baseline} == {fused}");
            // Console.WriteLine($"{Math.Abs(baseline - fusedVector) < TOLERANCE} {baseline} == {fusedVector}");

            var summary = BenchmarkRunner.Run<MultiplyAdd>();
        }
    }
}
