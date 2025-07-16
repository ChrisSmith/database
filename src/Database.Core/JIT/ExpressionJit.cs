using System.Reflection.Emit;

namespace Database.Core.JIT;

public class ExpressionJit
{

    private Dictionary<char, OpCode> _opCodesMappings = new()
    {
        {'+', OpCodes.Add},
        {'-', OpCodes.Sub},
        {'*', OpCodes.Mul},
        {'/', OpCodes.Div},
        {'%', OpCodes.Rem},
    };

    public static DynamicMethod FusedMultiplyAdd(bool debug = false)
    {
        var method1 = new DynamicMethod("Method1", typeof(double),
            [typeof(double[]), typeof(double[])]);

        var il = method1.GetILGenerator();

        // CLR IL uses a stack based execution model
        // https://kzdev.net/introduction-to-il/

        il.DeclareLocal(typeof(int), false); // i loc_0
        il.DeclareLocal(typeof(int), false); // length loc_1
        il.DeclareLocal(typeof(double), false); // res loc_2

        // TODO Try splitting the loop into sections like they have theirs
        // it might be causing the asm to be more predicable

        // int = 0;
        il.Emit(OpCodes.Ldc_I4_0);
        il.Emit(OpCodes.Stloc_0); // i loc_0

        // loc_1 = loop length
        il.Emit(OpCodes.Ldarg_0); // load the array length
        il.Emit(OpCodes.Ldlen); // n iterations
        il.Emit(OpCodes.Stloc_1); // length loc_1 = n

        // loc_2 res
        il.Emit(opcode: OpCodes.Ldc_R8, arg: 0d);
        il.Emit(OpCodes.Stloc_2);

        var loopCondition = il.DefineLabel();
        var loopBody = il.DefineLabel();

        il.Emit(OpCodes.Br_S, loopCondition); // check the loop condition first

        il.MarkLabel(loopBody); // start of loop body
        il.Emit(OpCodes.Ldloc_2); // load res

        // loop body
        il.Emit(OpCodes.Ldarg_0); // left array
        il.Emit(OpCodes.Ldloc_0);
        il.Emit(OpCodes.Ldelem_R8);

        il.Emit(OpCodes.Ldarg_1); // right array
        il.Emit(OpCodes.Ldloc_0);
        il.Emit(OpCodes.Ldelem_R8);

        il.Emit(OpCodes.Mul);
        il.Emit(OpCodes.Add); // res has been sitting on the stack, add to it (fused)
        il.Emit(OpCodes.Stloc_2); // store res in loc_2;

        // loop increment
        il.Emit(OpCodes.Ldloc_0);
        il.Emit(OpCodes.Ldc_I4_1); // literal 1
        il.Emit(OpCodes.Add);
        il.Emit(OpCodes.Stloc_0); // loc_0 i = i + 1

        // I really like how easy it is to "inline" a whole program without
        // worrying about blowing stuff up.
        if (debug)
        {
            // Console.WriteLine();
            var wlParams = new[] { typeof(string), typeof(object), typeof(object) };
            var writeLineMi = typeof(Console).GetMethod("WriteLine", wlParams)!;

            // Console.WriteLine("i = {0} res = {1}", i, res);
            il.Emit(OpCodes.Ldstr, "i = {0} res = {1}");
            il.Emit(OpCodes.Ldloc_0);
            il.Emit(OpCodes.Box, typeof(int));
            il.Emit(OpCodes.Ldloc_2);
            il.Emit(OpCodes.Box, typeof(double));
            il.Emit(OpCodes.Call, writeLineMi);
        }

        // check condition
        il.MarkLabel(loopCondition);
        il.Emit(OpCodes.Ldloc_0); // i
        il.Emit(OpCodes.Ldarg_0); // left
        il.Emit(OpCodes.Ldlen); // len
        il.Emit(OpCodes.Blt, loopBody); // if (i < 4) goto loop boundary;

        il.Emit(OpCodes.Ldloc_2);
        il.Emit(OpCodes.Ret);

        return method1;
    }
}
