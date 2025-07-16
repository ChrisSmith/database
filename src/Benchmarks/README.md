# Optimizing IL

Notes on emitting IL so RyuJIT can optimizate it correctly
- Use similar constructs to what Rosyln (C#) compiler is emitting, so we get the same optimizations
- Ensure the stack is empty before pushing a new instruction boundary (think loop jumps) Failure to do so results in extra load/stores, this was a 2x improvement
- Separate the loop conditional checks from the body and use unconditional jump at the beginning of the loop. Unclear why this helps, but it was a ~10% improvement


| Method                             | Mean     | Error    | StdDev   | Ratio | RatioSD |
|----------------------------------- |---------:|---------:|---------:|------:|--------:|
| Benchmark_Multiply_Then_Add_Loop   | 981.6 us | 26.53 us | 30.56 us |  1.00 |    0.04 |
| Benchmark_MultiplyAdd_Fused        | 570.8 us |  2.34 us |  2.69 us |  0.58 |    0.02 |
| Benchmark_MultiplyAdd_Fused_Vector | 404.7 us |  1.43 us |  1.53 us |  0.41 |    0.01 |
| Benchmark_Jit_MultiplyAdd_Fused    | 567.6 us |  0.75 us |  0.83 us |  0.58 |    0.02 |
