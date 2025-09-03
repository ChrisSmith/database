using System.Collections;

namespace Database.Core.Functions;

public class TopHeap<T> : IEnumerable<T>
{
    private readonly IReadOnlyList<Type> _keyTypes;
    private readonly int _limit;
    private readonly List<Array> _keys;
    private readonly T[] _objects;

    public TopHeap(IReadOnlyList<Type> keyTypes, int limit)
    {
        _keyTypes = keyTypes;
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

            // shift array right, and insert
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
                var comp = key.CompareTo(last);
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
            position = Array.BinarySearch(_keys[k], leftFence, fenceLen, key);
            if (position < 0)
            {
                // Doesn't exist, so no need to continue looking at additional index levels
                position = ~position;
                return true;
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
