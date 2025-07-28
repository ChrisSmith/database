using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Functions;

namespace Database.Core.Expressions;

public class ExpressionInterpreter
{
    public IColumn Execute(BaseExpression exp, RowGroup rowGroup)
    {
        if (exp is BinaryExpression be)
        {
            var left = Execute(be.Left, rowGroup);
            var right = Execute(be.Right, rowGroup);
            return Execute(exp, be.BoundFunction!, left, right);
        }

        if (exp is FunctionExpression fe)
        {
            var args = new IColumn[fe.Args.Length];
            for (var i = 0; i < fe.Args.Length; i++)
            {
                args[i] = Execute(fe.Args[i], rowGroup);
            }

            return Execute(fe.BoundFunction!, args);
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

        if (exp.BoundFunction == null)
        {
            throw new ExpressionEvaluationException($"expression does not have BoundFunction bound for evaluation. {exp}");
        }

        throw new ExpressionEvaluationException($"expression {exp} is not supported for evaluation");
    }

    // Assumes the expression has already been bound
    public IColumn Execute(BaseExpression expr, IFunction fun, IColumn left, IColumn right)
    {
        Array outputArray = null;

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
        else if (fun is IFilterFunctionTwo<DateTime> dt)
        {
            outputArray = dt.Ok((DateTime[])left.ValuesArray, (DateTime[])right.ValuesArray);
        }
        else if (fun is IFilterFunctionTwo<DateOnly> dto)
        {
            outputArray = dto.Ok((DateOnly[])left.ValuesArray, (DateOnly[])right.ValuesArray);
        }
        else
        {
            throw new NotImplementedException($"Function {fun.GetType().Name} not implemented");
        }

        var column = ColumnHelper.CreateColumn(
            fun.ReturnType.ClrTypeFromDataType(),
            expr.Alias,
            outputArray
            );
        return column;
    }

    public IColumn Execute(IFunction fun, IColumn[] args)
    {
        // TODO need generic Invoke capability on IFunction?
        throw new NotImplementedException();

        // var outputArray = null!;
        //
        // var column = type.GetConstructors().Single().Invoke([
        //     "foo",
        //     -1,
        //     outputArray
        // ]);
        //
        // return (IColumn)column;
    }

    public void ExecuteAggregate(FunctionExpression expr, IAggregateFunction fun, RowGroup rowGroup, IAggregateState[] state)
    {
        if (expr.Args.Length != 1)
        {
            throw new ExpressionEvaluationException($"expected aggregate function {fun.GetType().Name} to have 1 argument got {expr.Args}");
        }
        var column = Execute(expr.Args[0], rowGroup);

        fun.InvokeNext(column.ValuesArray, state);
    }
}

public class ExpressionEvaluationException(string message) : Exception(message) { }
