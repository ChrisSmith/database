using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Functions;
using Database.Core.Planner;
using Database.Core.Types;

namespace Database.Core.Expressions;

public class ExpressionInterpreter
{
    public IColumn Execute(BaseExpression exp, RowGroup rowGroup, CancellationToken token)
    {
        if (exp is BinaryExpression be)
        {
            var left = Execute(be.Left, rowGroup, token);

            if (be.Operator == TokenType.IN)
            {
                if (be.Right.BoundFunction is not TableValuedFunction tvf)
                {
                    throw new ExpressionEvaluationException($"expected right hand side of IN expressions to be a table valued function. got {be.Right.GetType().Name}");
                }
                return ExecuteInStatement(rowGroup, be.Alias, left, tvf.Table, token);
            }

            var right = Execute(be.Right, rowGroup, token);
            return Execute(exp, be.BoundFunction!, left, right);
        }

        if (exp is CastExpression cast)
        {
            var col = Execute(cast.Expression, rowGroup, token);
            return Execute(exp, cast.BoundFunction!, col);
        }

        if (exp is UnaryExpression ue)
        {
            var col = Execute(ue.Expression, rowGroup, token);
            return Execute(exp, ue.BoundFunction!, col);
        }

        if (exp is BetweenExpression bt)
        {
            var value = Execute(bt.Value, rowGroup, token);
            var lower = Execute(bt.Lower, rowGroup, token);
            var upper = Execute(bt.Upper, rowGroup, token);
            return Execute(exp, bt.BoundFunction!, value, lower, upper);
        }

        if (exp is FunctionExpression fe)
        {
            var args = new IColumn[fe.Args.Length];
            for (var i = 0; i < fe.Args.Length; i++)
            {
                args[i] = Execute(fe.Args[i], rowGroup, token);
            }

            return Execute(fe, fe.BoundFunction!, args);
        }

        if (exp is CaseExpression ce)
        {
            return ExecuteCaseStatement(ce, rowGroup, token);
        }

        if (exp.BoundFunction is IFunctionWithRowGroup fun)
        {
            return fun.Execute(rowGroup, token);
        }

        if (exp.BoundFunction is IFunctionWithColumnLength literal)
        {
            var expectedRows = rowGroup.NumRows;
            return literal.Execute(expectedRows);
        }

        if (exp.BoundFunction == null)
        {
            throw new ExpressionEvaluationException($"expression does not have BoundFunction bound for evaluation. {exp}");
        }

        throw new ExpressionEvaluationException($"expression ({exp.GetType().Name}) ({exp?.BoundFunction.GetType().Name}) {exp} is not supported for evaluation");
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
        else if (fun is IScalarMathOne<Decimal15, int> smdmi)
        {
            outputArray = smdmi.Execute((Decimal15[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<Decimal15, long> smdml)
        {
            outputArray = smdml.Execute((Decimal15[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<Decimal15, float> smdfm)
        {
            outputArray = smdfm.Execute((Decimal15[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<Decimal15, double> smddm)
        {
            outputArray = smddm.Execute((Decimal15[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<Decimal15, Decimal15> smddcm)
        {
            outputArray = smddcm.Execute((Decimal15[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<int, Decimal15> smidcm)
        {
            outputArray = smidcm.Execute((int[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<long, Decimal15> smldcm)
        {
            outputArray = smldcm.Execute((long[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<float, Decimal15> smfdcm)
        {
            outputArray = smfdcm.Execute((float[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<double, Decimal15> smddcm2)
        {
            outputArray = smddcm2.Execute((double[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<Decimal38, int> smdmi38)
        {
            outputArray = smdmi38.Execute((Decimal38[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<Decimal38, long> smdml38)
        {
            outputArray = smdml38.Execute((Decimal38[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<Decimal38, float> smdfm38)
        {
            outputArray = smdfm38.Execute((Decimal38[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<Decimal38, double> smddm38)
        {
            outputArray = smddm38.Execute((Decimal38[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<Decimal38, Decimal38> smddcm38)
        {
            outputArray = smddcm38.Execute((Decimal38[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<int, Decimal38> smidcm38)
        {
            outputArray = smidcm38.Execute((int[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<long, Decimal38> smldcm38)
        {
            outputArray = smldcm38.Execute((long[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<float, Decimal38> smfdcm38)
        {
            outputArray = smfdcm38.Execute((float[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<double, Decimal38> smddcm238)
        {
            outputArray = smddcm238.Execute((double[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<Decimal15, Decimal38> smddcm23)
        {
            outputArray = smddcm23.Execute((Decimal15[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<string, DateOnly> smsdo)
        {
            outputArray = smsdo.Execute((string[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<string, DateTime> smsdt)
        {
            outputArray = smsdt.Execute((string[])col.ValuesArray);
        }
        else if (fun is IScalarMathOne<DateOnly, DateTime> smdodt)
        {
            outputArray = smdodt.Execute((DateOnly[])col.ValuesArray);
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
        else if (fun is IScalarMathTwo<Decimal15> stdm)
        {
            outputArray = stdm.Execute((Decimal15[])left.ValuesArray, (Decimal15[])right.ValuesArray);
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
        else if (fun is IFilterFunctionTwo<Decimal15> fdm)
        {
            outputArray = fdm.Ok((Decimal15[])left.ValuesArray, (Decimal15[])right.ValuesArray);
        }
        else if (fun is IFilterFunctionTwo<Decimal38> fdm38)
        {
            outputArray = fdm38.Ok((Decimal38[])left.ValuesArray, (Decimal38[])right.ValuesArray);
        }
        else if (fun is IFilterFunctionTwo<string> ffts)
        {
            outputArray = ffts.Ok((string[])left.ValuesArray, (string[])right.ValuesArray);
        }
        else if (fun is IFunctionTwo<string, DateTime, int> fftsti)
        {
            outputArray = fftsti.Execute((string[])left.ValuesArray, (DateTime[])right.ValuesArray);
        }
        else if (fun is IScalarMathTwoFull<Decimal15, Decimal38> stdm38)
        {
            outputArray = stdm38.Execute((Decimal15[])left.ValuesArray, (Decimal15[])right.ValuesArray);
        }
        else if (fun is IScalarMathTwo<Decimal38> smtd38)
        {
            outputArray = smtd38.Execute((Decimal38[])left.ValuesArray, (Decimal38[])right.ValuesArray);
        }
        else if (fun is IScalarMathTwoFull<Decimal38, double> stdmd)
        {
            outputArray = stdmd.Execute((Decimal38[])left.ValuesArray, (Decimal38[])right.ValuesArray);
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
        else if (fun is IFilterThreeColsThree<Decimal15> stdm)
        {
            outputArray = stdm.Ok((Decimal15[])value.ValuesArray, (Decimal15[])lower.ValuesArray, (Decimal15[])upper.ValuesArray);
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


    public IColumn Execute(BaseExpression expr, IFunction fun, IColumn[] args)
    {
        if (args.Length == 1)
        {
            return Execute(expr, fun, args[0]);
        }
        if (args.Length == 2)
        {
            return Execute(expr, fun, args[0], args[1]);
        }
        if (args.Length == 3)
        {
            return Execute(expr, fun, args[0], args[1], args[2]);
        }

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

    public void ExecuteAggregate(FunctionExpression expr, IAggregateFunction fun, RowGroup rowGroup, IAggregateState[] state, CancellationToken token)
    {
        if (expr.Args.Length != 1)
        {
            throw new ExpressionEvaluationException($"expected aggregate function {fun.GetType().Name} to have 1 argument got {expr.Args}");
        }
        var column = Execute(expr.Args[0], rowGroup, token);

        fun.InvokeNext(column.ValuesArray, state);
    }

    public IColumn ExecuteCaseStatement(CaseExpression caseExpr, RowGroup rowGroup, CancellationToken token)
    {
        var numRows = rowGroup.NumRows;
        var totalMatches = 0;
        var resultIsSet = new bool[numRows];
        var outputType = caseExpr.BoundDataType!.Value.ClrTypeFromDataType();
        var outputArray = Array.CreateInstance(outputType, numRows);

        for (var i = 0; i < caseExpr.Conditions.Count && totalMatches < numRows; i++)
        {
            var cond = caseExpr.Conditions[i];
            var condValue = Execute(cond, rowGroup, token);
            if (condValue.ValuesArray is not bool[] condition)
            {
                throw new ExpressionEvaluationException($"expected case statement condition to be boolean got {condValue.GetType().Name}");
            }

            var thenExpr = caseExpr.Results[i];
            var thenValue = Execute(thenExpr, rowGroup, token);
            if (thenValue.ValuesArray.GetType() != outputArray.GetType())
            {
                throw new ExpressionEvaluationException($"expected case statement result to be {outputArray.GetType().Name} got {thenValue.GetType().Name}");
            }

            for (var j = 0; j < condition.Length; j++)
            {
                if (!resultIsSet[j] && condition[j])
                {
                    resultIsSet[j] = true;
                    totalMatches++;
                    outputArray.SetValue(thenValue[j], j);
                }
            }
        }

        if (totalMatches != numRows)
        {
            var defaults = Execute(caseExpr.Default ?? throw new ExpressionEvaluationException("nullable returns from case statements not supported"), rowGroup, token);
            for (var i = 0; i < numRows; i++)
            {
                if (!resultIsSet[i])
                {
                    outputArray.SetValue(defaults[i], i);
                }
            }
        }

        return ColumnHelper.CreateColumn(
            outputType,
            caseExpr.Alias,
            outputArray
        );
    }

    private IColumn ExecuteInStatement(RowGroup rowGroup, string alias, IColumn left, MemoryBasedTable table, CancellationToken token)
    {
        var rgs = table.GetRowGroups();
        if (rgs.Count != 1)
        {
            throw new Exception($"In Subquery must return a single row group, got {rgs.Count}");
        }

        var columnRef = table.Schema.Single().ColumnRef;
        var column = table.GetColumn(columnRef with { RowGroup = rgs[0] });

        if (left.Type != column.Type)
        {
            throw new Exception($"In Subquery column type {column.Type} does not match left column type {left.Type}");
        }

        Array outputArray = column.Type.DataTypeFromClrType() switch
        {
            DataType.Int => Contains((int[])left.ValuesArray, (int[])column.ValuesArray),
            DataType.Long => Contains((long[])left.ValuesArray, (long[])column.ValuesArray),
            DataType.Float => Contains((float[])left.ValuesArray, (float[])column.ValuesArray),
            DataType.Double => Contains((double[])left.ValuesArray, (double[])column.ValuesArray),
            DataType.String => Contains((string[])left.ValuesArray, (string[])column.ValuesArray),
            DataType.Date => Contains((DateOnly[])left.ValuesArray, (DateOnly[])column.ValuesArray),
            DataType.DateTime => Contains((DateTime[])left.ValuesArray, (DateTime[])column.ValuesArray),
            DataType.Decimal15 => Contains((Decimal15[])left.ValuesArray, (Decimal15[])column.ValuesArray),
            DataType.Bool => Contains((bool[])left.ValuesArray, (bool[])column.ValuesArray),
            _ => throw new NotImplementedException($"In Subquery column type {column.Type} not implemented")
        };

        return ColumnHelper.CreateColumn(
            typeof(bool),
            alias,
            outputArray
        );
    }

    private static bool[] Contains<T>(T[] source, T[] values) where T : IEquatable<T>
    {
        var results = new bool[source.Length];
        for (var i = 0; i < source.Length; i++)
        {
            results[i] = Contains(source[i], values);
        }
        return results;
    }

    private static bool Contains<T>(T source, T[] values) where T : IEquatable<T>
    {
        for (var i = 0; i < values.Length; i++)
        {
            if (values[i].Equals(source))
            {
                return true;
            }
        }
        return false;
    }
}

public class ExpressionEvaluationException(string message) : Exception(message) { }
