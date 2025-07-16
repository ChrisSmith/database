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
        // Console.WriteLine();
        var wlParams = new[] { typeof(string), typeof(object), typeof(object) };
        var writeLineMi = typeof(Console).GetMethod("WriteLine", wlParams)!;

        var method1 = new DynamicMethod("Method1", typeof(double),
            [typeof(double[]), typeof(double[])]);

        var il = method1.GetILGenerator();
        // il.EmitWriteLine("Method 1 here");

        // CLR IL uses a stack based execution model
        // https://kzdev.net/introduction-to-il/

        il.DeclareLocal(typeof(int), false); // i loc_0
        il.DeclareLocal(typeof(int), false); // length loc_1
        il.DeclareLocal(typeof(double), false); // res loc_2

        // int = 0;
        il.Emit(OpCodes.Ldc_I4_0);
        il.Emit(OpCodes.Stloc_0); // i loc_0

        // loc_1 = loop length
        il.Emit(OpCodes.Ldarg_0); // load the array length
        il.Emit(OpCodes.Ldlen); // n iterations
        il.Emit(OpCodes.Stloc_1); // length loc_1 = n

        // Drop this into the stack early so its there for the accumulation at the end
        // I'm assuming this will keep it in register instead of requiring a mem load
        il.Emit(OpCodes.Ldloc_2); // load res

        var loopBodyStart = il.DefineLabel();
        il.MarkLabel(loopBodyStart);

        // loop body
        il.Emit(OpCodes.Ldarg_0); // left array
        il.Emit(OpCodes.Ldloc_0);
        il.Emit(OpCodes.Ldelem_R8);

        il.Emit(OpCodes.Ldarg_1); // right array
        il.Emit(OpCodes.Ldloc_0);
        il.Emit(OpCodes.Ldelem_R8);

        il.Emit(OpCodes.Mul);
        il.Emit(OpCodes.Add); // res has been sitting on the stack, add to it (fused)

        // loop increment
        il.Emit(OpCodes.Ldloc_0);
        il.Emit(OpCodes.Ldc_I4_1); // literal 1
        il.Emit(OpCodes.Add);
        il.Emit(OpCodes.Stloc_0); // loc_0 i = i + 1

        // I really like how easy it is to "inline" a whole program without
        // worrying about blowing stuff up.
        if (debug)
        {
            // Console.WriteLine("i = {0} res = {1}", i, res);
            il.Emit(OpCodes.Ldstr, "i = {0} res = {1}");
            il.Emit(OpCodes.Ldloc_0);
            il.Emit(OpCodes.Box, typeof(int));
            il.Emit(OpCodes.Ldloc_2);
            il.Emit(OpCodes.Box, typeof(double));
            il.Emit(OpCodes.Call, writeLineMi);
        }

        // check condition
        il.Emit(OpCodes.Ldloc_0);
        il.Emit(OpCodes.Ldloc_1);
        il.Emit(OpCodes.Blt, loopBodyStart); // if (i < 4) goto loop boundary;

        // loc_2 is still on the stack at this point, ready to be returned directly
        il.Emit(OpCodes.Ret);

        return method1;
    }
}
