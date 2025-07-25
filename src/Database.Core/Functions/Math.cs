using System.Numerics;
using Database.Core.Catalog;

namespace Database.Core.Functions;

public interface IAggregateFunction : IFunction
{
    IAggregateState Initialize();

    IAggregateState[] InitializeArray(int size);

    object? GetValue(object state);

    void InvokeNext(object values, IAggregateState[] state);
}
/**
 * Operator on the entire column, one row at a time
 */
public interface IAggregateFunction<In, State, Out> : IAggregateFunction
{
    void Next(In[] value, State[] state);

    Out Value(State state);
}

public record Count<In> : IAggregateFunction<In, CountAggregateState, int>
    where In : INumber<In>
{
    public DataType ReturnType => DataType.Int;

    public int Value(CountAggregateState state) => state.Count;
    public object? GetValue(object state) => Value((CountAggregateState)state);

    public void InvokeNext(object values, IAggregateState[] state)
    {
        Next((In[])values, (CountAggregateState[])state);
    }

    public IAggregateState Initialize()
    {
        return new CountAggregateState();
    }

    public IAggregateState[] InitializeArray(int size)
    {
        return new CountAggregateState[size];
    }

    public void Next(In[] value, CountAggregateState[] state)
    {
        for (var i = 0; i < value.Length; i++)
        {
            state[i].Count += 1;
        }
    }
}

public record StringCount : IAggregateFunction<string?, CountAggregateState, int>
{
    public DataType ReturnType => DataType.String;

    public int Value(CountAggregateState state) => state.Count;

    public object? GetValue(object state) => Value((CountAggregateState)state);

    public IAggregateState Initialize()
    {
        return new CountAggregateState();
    }

    public IAggregateState[] InitializeArray(int size)
    {
        return new CountAggregateState[size];
    }

    public void InvokeNext(object values, IAggregateState[] state)
    {
        Next((string?[])values, (CountAggregateState[])state);
    }

    public void Next(string?[] value, CountAggregateState[] state)
    {
        for (var i = 0; i < value.Length; i++)
        {
            var item = value[i];
            if (item != null)
            {
                state[i].Count += 1;
            }
        }
    }
}

public record Sum<T>(DataType ReturnType) : IAggregateFunction<T, SumAggregateState<T>, T>
    where T : INumber<T>
{
    public T Value(SumAggregateState<T> state) => state.Sum;

    public object? GetValue(object state) => Value((SumAggregateState<T>)state);

    public IAggregateState Initialize()
    {
        return new SumAggregateState<T>();
    }

    public IAggregateState[] InitializeArray(int size)
    {
        return new SumAggregateState<T>[size];
    }

    public void InvokeNext(object values, IAggregateState[] state)
    {
        Next((T[])values, (SumAggregateState<T>[])state);
    }

    public void Next(T[] value, SumAggregateState<T>[] state)
    {
        for (var i = 0; i < value.Length; i++)
        {
            var item = value[i];
            state[i].Sum += item;
        }
    }
}

public interface IAggregateState
{
    public void Combine(IAggregateState other);
}

public class SumAggregateState<T> : IAggregateState
    where T : INumber<T>
{
    public T Sum { get; set; } = default!;

    public void Combine(IAggregateState other)
    {
        this.Sum += ((SumAggregateState<T>)other).Sum;
    }
}

public class CountAggregateState : IAggregateState
{
    public int Count { get; set; } = 0;

    public void Combine(IAggregateState other)
    {
        this.Count += ((CountAggregateState)other).Count;
    }
}

public class AvgState<T> : IAggregateState
    where T : INumber<T>
{
    public T Sum = default!;
    public int Count = 0;

    public double Value()
    {
        return (double)Convert.ChangeType(Sum, typeof(double))! / Count;
    }

    public void Combine(IAggregateState other)
    {
        this.Count += ((AvgState<T>)other).Count;
        this.Sum += ((AvgState<T>)other).Sum;
    }
}

public record Avg<T> : IAggregateFunction<T, AvgState<T>, double>
    where T : INumber<T>
{
    public DataType ReturnType => DataType.Double;

    public double Value(AvgState<T> state) => state.Value();

    public object? GetValue(object state) => Value((AvgState<T>)state);

    public IAggregateState Initialize()
    {
        return new AvgState<T>();
    }

    public IAggregateState[] InitializeArray(int size)
    {
        // ReSharper disable once CoVariantArrayConversion
        return new AvgState<T>[size];
    }


    public void InvokeNext(object values, IAggregateState[] state)
    {
        Next((T[])values, (AvgState<T>[])state);
    }

    public void Next(T[] value, AvgState<T>[] state)
    {
        for (var i = 0; i < value.Length; i++)
        {
            var item = value[i];
            state[i].Sum += item;
            state[i].Count += 1;
        }
    }
}
