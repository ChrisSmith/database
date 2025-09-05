using System.Collections;

namespace Database.Core.Functions;

public enum SortOrder { Ascending, Descending }

public class TopHeap<T> : IEnumerable<T>
{
    private readonly IReadOnlyList<Type> _keyTypes;
    private readonly int _limit;
    private readonly List<Array> _keys;
    private readonly T[] _objects;
    private readonly IReadOnlyList<SortOrder> _sortOrder;
    private readonly IReadOnlyList<IComparer> _comparers;

    public TopHeap(IReadOnlyList<Type> keyTypes, IReadOnlyList<SortOrder> sortOrder, int limit)
    {
        if (keyTypes.Count != sortOrder.Count)
        {
            throw new ArgumentException($"keyTypes and sortOrder must have the same number of elements. " +
                                        $"Got {keyTypes.Count} and {sortOrder.Count} respectively.");
        }

        var inverted = new InvertedComparer();
        _comparers = sortOrder.Select(s => s == SortOrder.Ascending ? (IComparer)Comparer.Default : inverted).ToArray();

        _keyTypes = keyTypes;
        _sortOrder = sortOrder;
        _limit = limit;
        _keys = InitializeKeysArray(limit);
        _objects = new T[limit];
    }

    public int Size { get; private set; } = 0;

    public void Insert(IReadOnlyList<Array> keys, T[] values)
    {
        if (keys.Count != _keyTypes.Count)
        {
            throw new ArgumentException($"keys must have the same number of elements as the key types. Expected {_keyTypes.Count} but got {keys.Count}.");
        }

        for (var i = 0; i < values.Length; i++)
        {
            if (!InsertPosition(keys, i, out var position))
            {
                continue;
            }

            var value = values[i];
            // Console.WriteLine($"Inserting {value} at position {position} with keys: {string.Join(", ", keys.Select(k => k.GetValue(i)))}");

            var len = Size - position;
            if (len > 0)
            {
                if (position + 1 + len > _limit)
                {
                    len -= 1;
                }

                for (var k = 0; k < _keys.Count; k++)
                {
                    Array.Copy(_keys[k], position, _keys[k], position + 1, len);
                }
                Array.Copy(_objects, position, _objects, position + 1, len);
            }

            for (var k = 0; k < _keys.Count; k++)
            {
                var key = (IComparable)keys[k].GetValue(i)!;
                _keys[k].SetValue(key, position);
            }

            _objects[position] = value;

            if (Size < _limit)
            {
                Size++;
            }
        }
    }

    private bool InsertPosition(IReadOnlyList<Array> keys, int i, out int position)
    {
        position = 0;

        if (Size == _limit)
        {
            for (var k = 0; k < keys.Count; k++)
            {
                var key = (IComparable)keys[k].GetValue(i)!;
                var last = (IComparable)_keys[k].GetValue(Size - 1)!;
                var comp = _comparers[k].Compare(key, last);
                if (comp > 0)
                {
                    // Larger than the last element, just skip it
                    return false;
                }

                if (comp < 0)
                {
                    // Smaller than the last element, we need to shift the array
                    // move onto search
                    break;
                }
                // Equal, check the next part of the composite index
            }
        }

        var leftFence = 0;
        var fenceLen = Size;
        for (var k = 0; k < keys.Count; k++)
        {
            var key = (IComparable)keys[k].GetValue(i)!;
            position = Array.BinarySearch(_keys[k], leftFence, fenceLen, key, _comparers[k]);
            // We need to know the sort order to determine if we need to push left or right

            if (position < 0)
            {
                // Doesn't exist, so no need to continue looking at additional index levels
                position = ~position;
                return true;
            }

            // Might not be the first matching element, move left
            while (position > 0 && _keys[k].GetValue(position - 1)!.Equals(key))
            {
                position -= 1;
            }

            fenceLen = 1;
            while (position + fenceLen < Size && _keys[k].GetValue(position + fenceLen)!.Equals(key))
            {
                fenceLen += 1;
            }

            leftFence = position;
        }

        return true;
    }

    private List<Array> InitializeKeysArray(int size)
    {
        var result = new List<Array>(_keyTypes.Count);
        for (var i = 0; i < _keyTypes.Count; i++)
        {
            var type = _keyTypes[i];
            var array = Array.CreateInstance(type, size);
            result.Add(array);
        }
        return result;
    }

    public IEnumerator<T> GetEnumerator()
    {
        for (var i = 0; i < Size; i++)
        {
            yield return _objects[i];
        }
    }

    public IEnumerable<object> GetKeys(int index)
    {
        for (var i = 0; i < Size; i++)
        {
            yield return _keys[index].GetValue(i)!;
        }
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}

public class InvertedComparer : IComparer
{
    public int Compare(object? x, object? y)
    {
        return Comparer.Default.Compare(y, x);
    }
}
