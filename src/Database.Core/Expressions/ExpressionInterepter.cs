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

        if (exp is CastExpression cast)
        {
            var col = Execute(cast.Expression, rowGroup);
            return Execute(exp, cast.BoundFunction!, col);
        }

        if (exp is UnaryExpression ue)
        {
            var col = Execute(ue.Expression, rowGroup);
            return Execute(exp, ue.BoundFunction!, col);
        }

        if (exp is BetweenExpression bt)
        {
            var value = Execute(bt.Value, rowGroup);
            var lower = Execute(bt.Lower, rowGroup);
            var upper = Execute(bt.Upper, rowGroup);
            return Execute(exp, bt.BoundFunction!, value, lower, upper);
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

    public IColumn Execute(BaseExpression expr, IFunction fun, IColumn col)
    {
        Array outputArray = null;

        if (fun is IScalarMathOne<int, int> smii)
        {
            outputArray = smii.Execute((int[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<long, long> smll)
        {
            outputArray = smll.Execute((long[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<float, float> smff)
        {
            outputArray = smff.Execute((float[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<double, double> smdd)
        {
            outputArray = smdd.Execute((double[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<int, long> smil)
        {
            outputArray = smil.Execute((int[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<long, int> smli)
        {
            outputArray = smli.Execute((long[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<float, double> smfd)
        {
            outputArray = smfd.Execute((float[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<int, float> smif)
        {
            outputArray = smif.Execute((int[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<int, double> smid)
        {
            outputArray = smid.Execute((int[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<long, float> smlf)
        {
            outputArray = smlf.Execute((long[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<long, double> smld)
        {
            outputArray = smld.Execute((long[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<double, int> smdi)
        {
            outputArray = smdi.Execute((double[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<double, long> smdl)
        {
            outputArray = smdl.Execute((double[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<float, int> smfi)
        {
            outputArray = smfi.Execute((float[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<float, long> smfl)
        {
            outputArray = smfl.Execute((float[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<double, float> smdf)
        {
            outputArray = smdf.Execute((double[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<decimal, int> smdmi)
        {
            outputArray = smdmi.Execute((decimal[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<decimal, long> smdml)
        {
            outputArray = smdml.Execute((decimal[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<decimal, float> smdfm)
        {
            outputArray = smdfm.Execute((decimal[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<decimal, double> smddm)
        {
            outputArray = smddm.Execute((decimal[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<decimal, decimal> smddcm)
        {
            outputArray = smddcm.Execute((decimal[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<int, decimal> smidcm)
        {
            outputArray = smidcm.Execute((int[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<long, decimal> smldcm)
        {
            outputArray = smldcm.Execute((long[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<float, decimal> smfdcm)
        {
            outputArray = smfdcm.Execute((float[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<double, decimal> smddcm2)
        {
            outputArray = smddcm2.Execute((double[])col.ValuesArray);
        }
        else if (fun is IFilterFunctionOne<bool> fb)
        {
            outputArray = fb.Ok((bool[])col.ValuesArray);
        }
        else
        {
            throw new NotImplementedException($"Unary function {fun.GetType().Name} not implemented");
        }

        var column = ColumnHelper.CreateColumn(
            fun.ReturnType.ClrTypeFromDataType(),
            expr.Alias,
            outputArray
        );
        return column;
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
        else if (fun is IScalarMathTwo<decimal> stdm)
        {
            outputArray = stdm.Execute((decimal[])left.ValuesArray, (decimal[])right.ValuesArray);
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
        else if (fun is IFilterFunctionTwo<bool> fb)
        {
            outputArray = fb.Ok((bool[])left.ValuesArray, (bool[])right.ValuesArray);
        }
        else if (fun is IFilterFunctionTwo<decimal> fdm)
        {
            outputArray = fdm.Ok((decimal[])left.ValuesArray, (decimal[])right.ValuesArray);
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

    public IColumn Execute(BaseExpression expr, IFunction fun, IColumn value, IColumn lower, IColumn upper)
    {
        Array outputArray = null;

        if (fun is IFilterThreeColsThree<int> sti)
        {
            outputArray = sti.Ok((int[])value.ValuesArray, (int[])lower.ValuesArray, (int[])upper.ValuesArray);
        }
        else if (fun is IFilterThreeColsThree<long> stl)
        {
            outputArray = stl.Ok((long[])value.ValuesArray, (long[])lower.ValuesArray, (long[])upper.ValuesArray);
        }
        else if (fun is IFilterThreeColsThree<float> stf)
        {
            outputArray = stf.Ok((float[])value.ValuesArray, (float[])lower.ValuesArray, (float[])upper.ValuesArray);
        }
        else if (fun is IFilterThreeColsThree<double> std)
        {
            outputArray = std.Ok((double[])value.ValuesArray, (double[])lower.ValuesArray, (double[])upper.ValuesArray);
        }
        else if (fun is IFilterThreeColsThree<decimal> stdm)
        {
            outputArray = stdm.Ok((decimal[])value.ValuesArray, (decimal[])lower.ValuesArray, (decimal[])upper.ValuesArray);
        }
        else if (fun is IFilterThreeColsThree<DateTime> dt)
        {
            outputArray = dt.Ok((DateTime[])value.ValuesArray, (DateTime[])lower.ValuesArray, (DateTime[])upper.ValuesArray);
        }
        else if (fun is IFilterThreeColsThree<DateOnly> dto)
        {
            outputArray = dto.Ok((DateOnly[])value.ValuesArray, (DateOnly[])lower.ValuesArray, (DateOnly[])upper.ValuesArray);
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
