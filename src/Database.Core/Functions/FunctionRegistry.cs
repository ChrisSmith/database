using Database.Core.Catalog;
using Database.Core.Expressions;

namespace Database.Core.Functions;

public record FunctionDefinition(string Name, int Arity, Dictionary<DataType, Type> Functions);

public class FunctionRegistry
{
    private readonly Dictionary<string, FunctionDefinition> _funcs = new(StringComparer.OrdinalIgnoreCase);

    public FunctionRegistry()
    {
        _funcs.Add("count", new("count", 1, new()
        {
            { DataType.Int, typeof(Count<int>)},
            { DataType.Long, typeof(Count<long>)},
            { DataType.Float, typeof(Count<float>)},
            { DataType.Double, typeof(Count<double>)},
            { DataType.String, typeof(StringCount)},
        }));

        _funcs.Add("sum", new("sum", 1, new()
        {
            { DataType.Int, typeof(Sum<int>)},
            { DataType.Long, typeof(Sum<long>)},
            { DataType.Float, typeof(Sum<float>)},
            { DataType.Double, typeof(Sum<double>)},
        }));

        _funcs.Add("avg", new("avg", 1, new()
        {
            { DataType.Int, typeof(Avg<int>)},
            { DataType.Long, typeof(Avg<long>)},
            { DataType.Float, typeof(Avg<float>)},
            { DataType.Double, typeof(Avg<double>)},
        }));

        _funcs.Add("=", new("=", 2, new()
        {
            { DataType.Int, typeof(EqualTwo<int>) },
            { DataType.Long, typeof(EqualTwo<long>) },
            { DataType.Float, typeof(EqualTwo<float>) },
            { DataType.Double, typeof(EqualTwo<double>) },
            { DataType.Date, typeof(EqualTwoDateOnly) },
            { DataType.DateTime, typeof(EqualTwoDateTime) },
        }));
        _funcs.Add(">", new(">", 2, new()
        {
            { DataType.Int, typeof(GreaterThanTwo<int>) },
            { DataType.Long, typeof(GreaterThanTwo<long>) },
            { DataType.Float, typeof(GreaterThanTwo<float>) },
            { DataType.Double, typeof(GreaterThanTwo<double>) },
            { DataType.Date, typeof(GreaterThanTwoDateOnly) },
            { DataType.DateTime, typeof(GreaterThanTwoDateTime) },
        }));
        _funcs.Add(">=", new(">=", 2, new()
        {
            { DataType.Int, typeof(GreaterThanEqualTwo<int>) },
            { DataType.Long, typeof(GreaterThanEqualTwo<long>) },
            { DataType.Float, typeof(GreaterThanEqualTwo<float>) },
            { DataType.Double, typeof(GreaterThanEqualTwo<double>) },
            { DataType.Date, typeof(GreaterThanEqualTwoDateOnly) },
            { DataType.DateTime, typeof(GreaterThanEqualTwoDateTime) },
        }));
        _funcs.Add("<", new("<", 2, new()
        {
            { DataType.Int, typeof(LessThanTwo<int>) },
            { DataType.Long, typeof(LessThanTwo<long>) },
            { DataType.Float, typeof(LessThanTwo<float>) },
            { DataType.Double, typeof(LessThanTwo<double>) },
            { DataType.Date, typeof(LessThanTwoDateOnly) },
            { DataType.DateTime, typeof(LessThanTwoDateTime) },
        }));
        _funcs.Add("<=", new("<=", 2, new()
        {
            { DataType.Int, typeof(LessThanEqualTwo<int>) },
            { DataType.Long, typeof(LessThanEqualTwo<long>) },
            { DataType.Float, typeof(LessThanEqualTwo<float>) },
            { DataType.Double, typeof(LessThanEqualTwo<double>) },
            { DataType.Date, typeof(LessThanEqualTwoDateOnly) },
            { DataType.DateTime, typeof(LessThanEqualTwoDateTime) },
        }));
        _funcs.Add("*", new("*", 2, new()
        {
            { DataType.Int, typeof(MultiplyTwo<int>) },
            { DataType.Long, typeof(MultiplyTwo<long>) },
            { DataType.Float, typeof(MultiplyTwo<float>) },
            { DataType.Double, typeof(MultiplyTwo<double>) },
        }));
        _funcs.Add("/", new("/", 2, new()
        {
            { DataType.Int, typeof(DivideTwo<int>) },
            { DataType.Long, typeof(DivideTwo<long>) },
            { DataType.Float, typeof(DivideTwo<float>) },
            { DataType.Double, typeof(DivideTwo<double>) },
        }));
        _funcs.Add("-", new("-", 2, new()
        {
            { DataType.Int, typeof(MinusTwo<int>) },
            { DataType.Long, typeof(MinusTwo<long>) },
            { DataType.Float, typeof(MinusTwo<float>) },
            { DataType.Double, typeof(MinusTwo<double>) },
        }));
        _funcs.Add("+", new("+", 2, new()
        {
            { DataType.Int, typeof(SumTwo<int>) },
            { DataType.Long, typeof(SumTwo<long>) },
            { DataType.Float, typeof(SumTwo<float>) },
            { DataType.Double, typeof(SumTwo<double>) },
        }));
        _funcs.Add("%", new("%", 2, new()
        {
            { DataType.Int, typeof(ModuloTwo<int>) },
            { DataType.Long, typeof(ModuloTwo<long>) },
            { DataType.Float, typeof(ModuloTwo<float>) },
            { DataType.Double, typeof(ModuloTwo<double>) },
        }));
    }

    public IFunction BindFunction(string name, IExpression[] args, TableSchema table)
    {
        if (!_funcs.TryGetValue(name, out var func))
        {
            throw new FunctionBindException($"function '{name}' is not registered");
        }

        if (func.Arity != args.Length)
        {
            throw new FunctionBindException($"function '{name}' expects {func.Arity} arguments, got {args.Length}");
        }

        var firstArgument = args.First();
        var dataType = firstArgument.BoundDataType!.Value;

        if (!func.Functions.TryGetValue(dataType, out var funcType))
        {
            throw new FunctionBindException($"{dataType} not supported for {func.Name}");
        }

        var ctorArgs = new object[] { dataType };

        var ctor = funcType.GetConstructors().Single();
        if (ctor.GetParameters().Length == 0)
        {
            ctorArgs = [];
        }

        return (IFunction)ctor.Invoke(ctorArgs);
    }
}

public class FunctionBindException(string message) : Exception(message) { }
