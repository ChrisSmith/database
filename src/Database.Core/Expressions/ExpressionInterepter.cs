using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Functions;

namespace Database.Core.Expressions;

public class ExpressionInterpreter
{
    public IColumn Execute(IExpression exp, RowGroup rowGroup)
    {
        if (exp is BinaryExpression be)
        {
            var left = Execute(be.Left, rowGroup);
            var right = Execute(be.Right, rowGroup);
            return Execute(be.BoundFunction!, left, right);
        }

        if (exp.BoundFunction is SelectFunction select)
        {
            return select.SelectColumn(rowGroup);
        }

        if (exp.BoundFunction is LiteralFunction literal)
        {
            var expectedRows = rowGroup.NumRows;
            return literal.MaterializeColumn(expectedRows);
        }

        throw new ExpressionEvaluationException($"expression {exp} is not supported for evaluation");
    }

    // Assumes the expression has already been bound
    public IColumn Execute(IFunction fun, IColumn left, IColumn right)
    {
        var type = typeof(Column<>).MakeGenericType(fun.ReturnType.ClrTypeFromDataType());

        object outputArray = null;

        if (fun is IScalarMathTwo<int> sti)
        {
            outputArray = sti.Execute((int[])left.ValuesArray, (int[])right.ValuesArray);
        }
        else if (fun is IScalarMathTwo<long> stl)
        {
            outputArray = stl.Execute((long[])left.ValuesArray, (long[])right.ValuesArray);
        }
        else if (fun is IScalarMathTwo<float> stf)
        {
            outputArray = stf.Execute((float[])left.ValuesArray, (float[])right.ValuesArray);
        }
        else if (fun is IScalarMathTwo<double> std)
        {
            outputArray = std.Execute((double[])left.ValuesArray, (double[])right.ValuesArray);
        }
        else if (fun is IFilterFunctionTwo<int> fi)
        {
            outputArray = fi.Ok((int[])left.ValuesArray, (int[])right.ValuesArray);
        }
        else if (fun is IFilterFunctionTwo<long> fl)
        {
            outputArray = fl.Ok((long[])left.ValuesArray, (long[])right.ValuesArray);
        }
        else if (fun is IFilterFunctionTwo<float> ff)
        {
            outputArray = ff.Ok((float[])left.ValuesArray, (float[])right.ValuesArray);
        }
        else if (fun is IFilterFunctionTwo<double> fd)
        {
            outputArray = fd.Ok((double[])left.ValuesArray, (double[])right.ValuesArray);
        }
        else
        {
            throw new NotImplementedException($"Function {fun.GetType().Name} not implemented");
        }

        var column = type.GetConstructors().Single().Invoke([
            "foo",
            -1,
            outputArray
        ]);

        return (IColumn)column;
    }

    public void ExecuteAggregate(IAggregateFunction fun, RowGroup rowGroup)
    {
        // TODO fix
        var column = rowGroup.Columns.First();

        switch (fun)
        {
            case IAggregateFunction<double, int> agg when column is Column<double> c:
                agg.Next(c.Values);
                break;
            case IAggregateFunction<int?, int> agg when column is Column<int?> c:
                agg.Next(c.Values);
                break;
            case IAggregateFunction<int, int> agg when column is Column<int> c:
                agg.Next(c.Values);
                break;
            case IAggregateFunction<int, double> agg when column is Column<int> c:
                agg.Next(c.Values);
                break;
            case IAggregateFunction<string, int> agg when column is Column<string> c:
                agg.Next(c.Values);
                break;
            default:
                throw new ExpressionEvaluationException($"aggregate function {fun.GetType().Name} not implemented");
        }
    }
}

public class ExpressionEvaluationException(string message) : Exception(message) { }
