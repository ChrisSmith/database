using System.Diagnostics.Contracts;
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
            { DataType.Decimal, typeof(Count<decimal>)},
            { DataType.String, typeof(StringCount)},
        }));

        _funcs.Add("sum", new("sum", 1, new()
        {
            { DataType.Int, typeof(Sum<int>)},
            { DataType.Long, typeof(Sum<long>)},
            { DataType.Float, typeof(Sum<float>)},
            { DataType.Double, typeof(Sum<double>)},
            { DataType.Decimal, typeof(Sum<decimal>)},
        }));

        _funcs.Add("avg", new("avg", 1, new()
        {
            { DataType.Int, typeof(Avg<int>)},
            { DataType.Long, typeof(Avg<long>)},
            { DataType.Float, typeof(Avg<float>)},
            { DataType.Double, typeof(Avg<double>)},
            { DataType.Decimal, typeof(Avg<decimal>)},
        }));
        _funcs.Add("max", new("max", 1, new()
        {
            { DataType.Int, typeof(Max<int>)},
            { DataType.Long, typeof(Max<long>)},
            { DataType.Float, typeof(Max<float>)},
            { DataType.Double, typeof(Max<double>)},
            { DataType.Decimal, typeof(Max<decimal>)},
        }));
        _funcs.Add("min", new("min", 1, new()
        {
            { DataType.Int, typeof(Min<int>)},
            { DataType.Long, typeof(Min<long>)},
            { DataType.Float, typeof(Min<float>)},
            { DataType.Double, typeof(Min<double>)},
            { DataType.Decimal, typeof(Min<decimal>)},
        }));

        _funcs.Add("=", new("=", 2, new()
        {
            { DataType.Int, typeof(EqualTwo<int>) },
            { DataType.Long, typeof(EqualTwo<long>) },
            { DataType.Float, typeof(EqualTwo<float>) },
            { DataType.Double, typeof(EqualTwo<double>) },
            { DataType.Date, typeof(EqualTwoDateOnly) },
            { DataType.DateTime, typeof(EqualTwoDateTime) },
            { DataType.Decimal, typeof(EqualTwo<decimal>) },
            { DataType.String, typeof(EqualTwoString) },
        }));
        _funcs.Add("!=", new("!=", 2, new()
        {
            { DataType.Int, typeof(NotEqualTwo<int>) },
            { DataType.Long, typeof(NotEqualTwo<long>) },
            { DataType.Float, typeof(NotEqualTwo<float>) },
            { DataType.Double, typeof(NotEqualTwo<double>) },
            { DataType.Date, typeof(NotEqualTwoDateOnly) },
            { DataType.DateTime, typeof(NotEqualTwoDateTime) },
            { DataType.Decimal, typeof(NotEqualTwo<decimal>) },
            { DataType.String, typeof(NotEqualTwoString) },
        }));
        _funcs.Add(">", new(">", 2, new()
        {
            { DataType.Int, typeof(GreaterThanTwo<int>) },
            { DataType.Long, typeof(GreaterThanTwo<long>) },
            { DataType.Float, typeof(GreaterThanTwo<float>) },
            { DataType.Double, typeof(GreaterThanTwo<double>) },
            { DataType.Date, typeof(GreaterThanTwoDateOnly) },
            { DataType.DateTime, typeof(GreaterThanTwoDateTime) },
            { DataType.Decimal, typeof(GreaterThanTwo<decimal>) },
        }));
        _funcs.Add(">=", new(">=", 2, new()
        {
            { DataType.Int, typeof(GreaterThanEqualTwo<int>) },
            { DataType.Long, typeof(GreaterThanEqualTwo<long>) },
            { DataType.Float, typeof(GreaterThanEqualTwo<float>) },
            { DataType.Double, typeof(GreaterThanEqualTwo<double>) },
            { DataType.Date, typeof(GreaterThanEqualTwoDateOnly) },
            { DataType.DateTime, typeof(GreaterThanEqualTwoDateTime) },
            { DataType.Decimal, typeof(GreaterThanEqualTwo<decimal>) },
        }));
        _funcs.Add("<", new("<", 2, new()
        {
            { DataType.Int, typeof(LessThanTwo<int>) },
            { DataType.Long, typeof(LessThanTwo<long>) },
            { DataType.Float, typeof(LessThanTwo<float>) },
            { DataType.Double, typeof(LessThanTwo<double>) },
            { DataType.Date, typeof(LessThanTwoDateOnly) },
            { DataType.DateTime, typeof(LessThanTwoDateTime) },
            { DataType.Decimal, typeof(LessThanTwo<decimal>) },
        }));
        _funcs.Add("<=", new("<=", 2, new()
        {
            { DataType.Int, typeof(LessThanEqualTwo<int>) },
            { DataType.Long, typeof(LessThanEqualTwo<long>) },
            { DataType.Float, typeof(LessThanEqualTwo<float>) },
            { DataType.Double, typeof(LessThanEqualTwo<double>) },
            { DataType.Date, typeof(LessThanEqualTwoDateOnly) },
            { DataType.DateTime, typeof(LessThanEqualTwoDateTime) },
            { DataType.Decimal, typeof(LessThanEqualTwo<decimal>) },
        }));
        _funcs.Add("*", new("*", 2, new()
        {
            { DataType.Int, typeof(MultiplyTwo<int>) },
            { DataType.Long, typeof(MultiplyTwo<long>) },
            { DataType.Float, typeof(MultiplyTwo<float>) },
            { DataType.Double, typeof(MultiplyTwo<double>) },
            { DataType.Decimal, typeof(MultiplyTwo<decimal>) },
        }));
        _funcs.Add("/", new("/", 2, new()
        {
            { DataType.Int, typeof(DivideTwo<int>) },
            { DataType.Long, typeof(DivideTwo<long>) },
            { DataType.Float, typeof(DivideTwo<float>) },
            { DataType.Double, typeof(DivideTwo<double>) },
            { DataType.Decimal, typeof(DivideTwo<decimal>) },
        }));
        _funcs.Add("-", new("-", 2, new()
        {
            { DataType.Int, typeof(MinusTwo<int>) },
            { DataType.Long, typeof(MinusTwo<long>) },
            { DataType.Float, typeof(MinusTwo<float>) },
            { DataType.Double, typeof(MinusTwo<double>) },
            { DataType.Decimal, typeof(MinusTwo<decimal>) },
        }));
        _funcs.Add("+", new("+", 2, new()
        {
            { DataType.Int, typeof(SumTwo<int>) },
            { DataType.Long, typeof(SumTwo<long>) },
            { DataType.Float, typeof(SumTwo<float>) },
            { DataType.Double, typeof(SumTwo<double>) },
            { DataType.Decimal, typeof(SumTwo<decimal>) },
        }));
        _funcs.Add("%", new("%", 2, new()
        {
            { DataType.Int, typeof(ModuloTwo<int>) },
            { DataType.Long, typeof(ModuloTwo<long>) },
            { DataType.Float, typeof(ModuloTwo<float>) },
            { DataType.Double, typeof(ModuloTwo<double>) },
            { DataType.Decimal, typeof(ModuloTwo<decimal>) },
        }));
        _funcs.Add("between", new("between", 3, new()
        {
            { DataType.Int, typeof(Between<int>)},
            { DataType.Long, typeof(Between<long>)},
            { DataType.Float, typeof(Between<float>)},
            { DataType.Double, typeof(Between<double>)},
            { DataType.Decimal, typeof(Between<decimal>) },
            { DataType.DateTime, typeof(BetweenDateTime) },
        }));
        _funcs.Add("not_between", new("not_between", 3, new()
        {
            { DataType.Int, typeof(NotBetween<int>)},
            { DataType.Long, typeof(NotBetween<long>)},
            { DataType.Float, typeof(NotBetween<float>)},
            { DataType.Double, typeof(NotBetween<double>)},
            { DataType.Decimal, typeof(NotBetween<decimal>) },
            { DataType.DateTime, typeof(NotBetweenDateTime) },
        }));
        _funcs.Add("cast_int", new("cast_int", 1, new()
        {
            { DataType.Int, typeof(CastInt<int>)},
            { DataType.Long, typeof(CastInt<long>)},
            { DataType.Float, typeof(CastInt<float>)},
            { DataType.Double, typeof(CastInt<double>)},
            { DataType.Decimal, typeof(CastInt<decimal>) },
        }));
        _funcs.Add("cast_long", new("cast_long", 1, new()
        {
            { DataType.Int, typeof(CastLong<int>)},
            { DataType.Long, typeof(CastLong<long>)},
            { DataType.Float, typeof(CastLong<float>)},
            { DataType.Double, typeof(CastLong<double>)},
            { DataType.Decimal, typeof(CastLong<decimal>) },
        }));
        _funcs.Add("cast_float", new("cast_float", 1, new()
        {
            { DataType.Int, typeof(CastFloat<int>)},
            { DataType.Long, typeof(CastFloat<long>)},
            { DataType.Float, typeof(CastFloat<float>)},
            { DataType.Double, typeof(CastFloat<double>)},
            { DataType.Decimal, typeof(CastFloat<decimal>) },
        }));
        _funcs.Add("cast_double", new("cast_double", 1, new()
        {
            { DataType.Int, typeof(CastDouble<int>)},
            { DataType.Long, typeof(CastDouble<long>)},
            { DataType.Float, typeof(CastDouble<float>)},
            { DataType.Double, typeof(CastDouble<double>)},
            { DataType.Decimal, typeof(CastDouble<decimal>) },
        }));
        _funcs.Add("cast_decimal", new("cast_decimal", 1, new()
        {
            { DataType.Int, typeof(CastDecimal<int>)},
            { DataType.Long, typeof(CastDecimal<long>)},
            { DataType.Float, typeof(CastDecimal<float>)},
            { DataType.Double, typeof(CastDecimal<double>) },
        }));
        _funcs.Add("cast_datetime", new("cast_datetime", 1, new()
        {
            { DataType.Date, typeof(CastDateToDateTime)},
        }));
        _funcs.Add("and", new("and", 2, new()
        {
            { DataType.Bool, typeof(LogicalAnd) },
        }));
        _funcs.Add("or", new("or", 2, new()
        {
            { DataType.Bool, typeof(LogicalOr) },
        }));
        _funcs.Add("not", new("not", 1, new()
        {
            { DataType.Bool, typeof(LogicalNot) },
        }));
        _funcs.Add("like", new("like", 2, new()
        {
            { DataType.String, typeof(DynamicLike) },
        }));
        _funcs.Add("starts_with", new("starts_with", 2, new()
        {
            { DataType.String, typeof(StartsWithTwo) },
        }));
        _funcs.Add("ends_with", new("ends_with", 2, new()
        {
            { DataType.String, typeof(EndsWithTwo) },
        }));
        _funcs.Add("extract", new("extract", 2, new()
        {
            { DataType.String, typeof(ExtractPart) },
        }));
        _funcs.Add("date", new("date", 1, new()
        {
            { DataType.String, typeof(CreateDate) },
        }));
    }

    [Pure]
    public IFunction BindFunction(string name, BaseExpression[] args)
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
            throw new FunctionBindException($"datatype {dataType} not supported for function {func.Name}");
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
