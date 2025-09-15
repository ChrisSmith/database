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

public record CountDistinct<T> : IAggregateFunction<T, CountDistinctState<T>, int>
{
    public DataType ReturnType => DataType.Int;

    public int Value(CountDistinctState<T> state) => state.Count;

    public object? GetValue(object state) => Value((CountDistinctState<T>)state);

    public IAggregateState Initialize()
    {
        return new CountDistinctState<T>();
    }

    public IAggregateState[] InitializeArray(int size)
    {
        return new CountDistinctState<T>[size];
    }

    public void InvokeNext(object values, IAggregateState[] state)
    {
        Next((T[])values, (CountDistinctState<T>[])state);
    }

    public void Next(T[] value, CountDistinctState<T>[] state)
    {
        for (var i = 0; i < value.Length; i++)
        {
            var item = value[i];
            state[i].HashSet.Add(item);
        }
    }
}

public record StringMax : IAggregateFunction<string?, State<string>, string>
{
    public DataType ReturnType => DataType.String;

    public string Value(State<string> state) => state.Value;

    public object? GetValue(object state) => Value((State<string>)state);

    public IAggregateState Initialize()
    {
        return new State<string>();
    }

    public IAggregateState[] InitializeArray(int size)
    {
        return new State<string>[size];
    }

    public void InvokeNext(object values, IAggregateState[] state)
    {
        Next((string?[])values, (State<string>[])state);
    }

    public void Next(string?[] value, State<string>[] state)
    {
        for (var i = 0; i < value.Length; i++)
        {
            var item = value[i];
            var current = state[i].Value;
            if (current == null || item != null && current.CompareTo(item) < 0)
            {
                state[i].Value = item!;
            }
        }
    }
}

public record BoolMax : IAggregateFunction<bool, State<bool>, bool>
{
    public DataType ReturnType => DataType.Bool;

    public bool Value(State<bool> state) => state.Value;

    public object? GetValue(object state) => Value((State<bool>)state);

    public IAggregateState Initialize()
    {
        return new State<bool>();
    }

    public IAggregateState[] InitializeArray(int size)
    {
        return new State<bool>[size];
    }

    public void InvokeNext(object values, IAggregateState[] state)
    {
        Next((bool[])values, (State<bool>[])state);
    }

    public void Next(bool[] value, State<bool>[] state)
    {
        for (var i = 0; i < value.Length; i++)
        {
            var item = value[i];
            var current = state[i].Value;
            if (!current && item)
            {
                state[i].Value = item;
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
}

public class SumAggregateState<T> : IAggregateState
    where T : INumber<T>
{
    public T Sum { get; set; } = default!;
}

public class ScalarState<T> : IAggregateState
    where T : INumber<T>
{
    public T Value { get; set; } = default!;
}

public class State<T> : IAggregateState
{
    public T Value { get; set; } = default!;
}
public class CountAggregateState : IAggregateState
{
    public int Count { get; set; } = 0;
}

public class CountDistinctState<T> : IAggregateState
{
    public int Count => HashSet.Count;

    public HashSet<T> HashSet = new();
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

public record Max<T>(DataType ReturnType) : IAggregateFunction<T, ScalarState<T>, T>
    where T : INumber<T>, IMinMaxValue<T>
{
    public T Value(ScalarState<T> state) => state.Value;

    public object? GetValue(object state) => Value((ScalarState<T>)state);

    public IAggregateState Initialize()
    {
        return new ScalarState<T>()
        {
            Value = T.MinValue,
        };
    }

    public IAggregateState[] InitializeArray(int size)
    {
        return new ScalarState<T>[size];
    }

    public void InvokeNext(object values, IAggregateState[] state)
    {
        Next((T[])values, (ScalarState<T>[])state);
    }

    public void Next(T[] value, ScalarState<T>[] state)
    {
        for (var i = 0; i < value.Length; i++)
        {
            var item = value[i];
            state[i].Value = T.Max(state[i].Value, item);
        }
    }
}


public record Min<T>(DataType ReturnType) : IAggregateFunction<T, ScalarState<T>, T>
    where T : INumber<T>, IMinMaxValue<T>
{
    public T Value(ScalarState<T> state) => state.Value;

    public object? GetValue(object state) => Value((ScalarState<T>)state);

    public IAggregateState Initialize()
    {
        return new ScalarState<T>()
        {
            Value = T.MaxValue,
        };
    }

    public IAggregateState[] InitializeArray(int size)
    {
        return new ScalarState<T>[size];
    }

    public void InvokeNext(object values, IAggregateState[] state)
    {
        Next((T[])values, (ScalarState<T>[])state);
    }

    public void Next(T[] value, ScalarState<T>[] state)
    {
        for (var i = 0; i < value.Length; i++)
        {
            var item = value[i];
            state[i].Value = T.Min(state[i].Value, item);
        }
    }
}
