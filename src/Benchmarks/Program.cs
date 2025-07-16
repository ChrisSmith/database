using System.Numerics;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Database.Core.JIT;

namespace MyBenchmarks
{
    [SimpleJob(iterationCount: 10)]
    public class MultiplyAdd
    {
        private readonly double[] data;
        private readonly DynamicMethod _method;
        private readonly Func<double[], double[], double> _delegate;

        public MultiplyAdd()
        {
            var rand = new Random(42);

            int N = (int)(2 * rand.NextDouble() + 1000 * 1000);
            data = new double[N];
            for (var i = 0; i < N; i++)
            {
                data[i] = rand.NextDouble();
            }
            _method = ExpressionJit.FusedMultiplyAdd(debug: false);
            _delegate = (Func<double[], double[], double>)_method.CreateDelegate(typeof(Func<double[], double[], double>));
        }

        [Benchmark(Baseline = true)]
        public double Benchmark_Multiply_Then_Add_Loop()
        {
            return MultiplyAddLoop(data, data);
        }

        [Benchmark]
        public double Benchmark_MultiplyAdd_Fused()
        {
            return MultiplyAdd_Fused(data, data);
        }

        [Benchmark]
        public double Benchmark_MultiplyAdd_Fused_Vector()
        {
            return MultiplyAdd_Fused_Vector(data, data);
        }

        [Benchmark]
        public double Benchmark_Jit_MultiplyAdd_Fused()
        {
            // return (double)_method.Invoke(null, [data, data])!;
            return _delegate(data, data);
        }


        [MethodImpl(MethodImplOptions.NoInlining)]
        private static double MultiplyAddLoop(double[] left, double[] right)
        {
            var mult = Multiply(left, right);

            double res = 0;
            for (var i = 0; i < left.Length; i++)
            {
                res += mult[i];
            }
            return res;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static double[] Multiply(double[] left, double[] right)
        {
            double[] buff = new double[left.Length];
            for (var i = 0; i < left.Length; i++)
            {
                buff[i] = left[i] * right[i];
            }
            return buff;
        }


        [MethodImpl(MethodImplOptions.NoInlining)]
        private static double MultiplyAdd_Fused(double[] left, double[] right)
        {
            double res = 0;
            for (var i = 0; i < left.Length; i++)
            {
                res += left[i] * right[i];
            }
            return res;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static double MultiplyAdd_Fused_Vector(double[] left, double[] right)
        {
            var acc = new Vector<double>();
            for (var i = 0; i < left.Length; i += Vector<double>.Count)
            {
                var vec1 = new Vector<double>(left, i);
                var vec2 = new Vector<double>(right, i);
                acc = Vector.FusedMultiplyAdd(vec1, vec2, acc);
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
            // var jitFused = c.ExpressionJit_MultiplyAdd_Fused();
            //
            // const double TOLERANCE = 1e-6;
            // Console.WriteLine($"{Math.Abs(baseline - fused) < TOLERANCE} {baseline} == {fused}");
            // Console.WriteLine($"{Math.Abs(baseline - fusedVector) < TOLERANCE} {baseline} == {fusedVector}");
            // Console.WriteLine($"{Math.Abs(baseline - jitFused) < TOLERANCE} {baseline} == {jitFused}");
            //
            // double d=0;
            // for (var i = 0; i < 1000; i++)
            // {
            //     d += c.ExpressionJit_MultiplyAdd_Fused();
            // }
            // Console.WriteLine(d);
            var summary = BenchmarkRunner.Run<MultiplyAdd>();
        }
    }
}
