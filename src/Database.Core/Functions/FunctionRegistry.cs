using Database.Core.Catalog;
using Database.Core.Expressions;

namespace Database.Core.Functions;

public record FunctionDefinition(string Name, int Arity, Dictionary<DataType, Type> Functions);

public class FunctionRegistry
{
    private readonly Dictionary<string, FunctionDefinition> _aggregateFuncs = new(StringComparer.OrdinalIgnoreCase);

    public FunctionRegistry()
    {
        _aggregateFuncs.Add("count", new("count", 1, new()
        {
            { DataType.Int, typeof(IntCount)},
            { DataType.Double, typeof(DoubleCount)},
            { DataType.String, typeof(StringCount)},
        }));

        _aggregateFuncs.Add("sum", new("sum", 1, new()
        {
            { DataType.Int, typeof(IntSum)},
            { DataType.Double, typeof(DoubleSum)},
        }));
    }

    public AggregateValue Bind(string name, IExpression[] args, TableSchema table)
    {
        if (!_aggregateFuncs.TryGetValue(name, out var func))
        {
            throw new FunctionBindException($"function '{name}' is not registered");
        }

        if (func.Arity != args.Length)
        {
            throw new FunctionBindException($"function '{name}' expects {func.Arity} arguments, got {args.Length}");
        }

        // TODO probably not a single datatype, but an array of datatypes for the method signatiture
        // could also support multiple arity that way

        // TODO should expressions know what datatypes they're operating on?
        // Expressions probably also need to know what table/column.
        // Having the table as an argument to this function doesn't make a ton of sense
        // How should we handle nested invocations? foo(bar(col))
        var argument = args.First();
        if (argument is ColumnExpression columnExpr)
        {
            var index = table.Columns.FindIndex(c => c.Name == columnExpr.Column);
            var column = table.Columns[index];

            if (!func.Functions.TryGetValue(column.DataType, out var funcType))
            {
                throw new FunctionBindException($"{column.DataType} not supported for {func.Name}");
            }

            return (AggregateValue)funcType.GetConstructors().Single().Invoke([index]);
        }

        // TODO how do we want to handle constant values in expressions?
        throw new FunctionBindException($"{argument} not implemented yet");
    }
}

public class FunctionBindException(string message) : Exception(message) { }
