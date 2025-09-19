using System.Numerics;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Database.Core.JIT;

namespace MyBenchmarks;

[SimpleJob(iterationCount: 20)]
public class MultiplyAdd
{
    private readonly double[] data;
    private readonly DynamicMethod _method;
    public MultiplyAdd()
    {
        var rand = new Random(42);

        int N = 1000 * 1000;
        data = new double[N];
        for (var i = 0; i < N; i++)
        {
            data[i] = rand.NextDouble();
        }
        _method = ExpressionJit.FusedMultiplyAdd(debug: false);

        // var sw = Stopwatch.StartNew();
        // // Create a dynamic assembly & module
        // var asmName = new AssemblyName("MyDynamicAssembly");
        // var asmBuilder = AssemblyBuilder.DefineDynamicAssembly(asmName, AssemblyBuilderAccess.Run);
        // var moduleBuilder = asmBuilder.DefineDynamicModule("MyDynamicModule");
        // var typeBuilder = moduleBuilder.DefineType("MyDynamicType", TypeAttributes.Public | TypeAttributes.Class);
        // var methodBuilder = typeBuilder.DefineMethod(
        //     "FusedMultiplyAdd",
        //     MethodAttributes.Public | MethodAttributes.Static,
        //     typeof(double),
        //     new[] { typeof(double[]), typeof(double[]) });
        //
        // ExpressionJit.FusedMultiplyAdd(methodBuilder.GetILGenerator());
        // var dynamicType = typeBuilder.CreateType();
        // sw.Stop();
        // Console.WriteLine($"Dynamic type creation took {sw.ElapsedMilliseconds} ms");
        // _delegateFromAssembly = dynamicType.GetMethod("FusedMultiplyAdd")!.CreateDelegate<Func<double[], double[], double>>();
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
        return (double)_method.Invoke(null, [data, data])!;
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
