using Database.Core.Execution;

namespace Database.Core.Functions;

public class HashTable<T>
{
    public int Size { get; private set; }
    private object?[,] _keys;
    private T[] _objects;

    private int _keyColumns;

    public HashTable(int keyColumns, int size = 13)
    {
        _keyColumns = keyColumns;
        Size = 0;
        _keys = new object[keyColumns, size];
        _objects = new T[size];
    }

    public List<T> GetOrAdd(IReadOnlyList<IColumn> keys, Func<T> initializer)
    {
        var hashed = HashFunctions.Hash(keys).Values;
        var result = new List<T>(hashed.Length);

        for (var i = 0; i < hashed.Length; i++)
        {
            var idx = Math.Abs(hashed[i]) % _objects.Length;
            while (true)
            {
                if (_objects[idx] == null)
                {
                    Insert(idx, i);
                    break;
                }
                if (KeysMatch(keys, i, idx))
                {
                    result.Add(_objects[idx]);
                    break;
                }
                // linear probe
                idx = (idx + 1) % _objects.Length;
            }
        }

        return result;

        void Insert(int idx, int i)
        {
            var value = initializer();
            _objects[idx] = value;
            result.Add(value);
            for (var j = 0; j < keys.Count; j++)
            {
                var k = keys[j][i];
                _keys[j, idx] = k;
            }
            Size++;
            ResizeMaybe();
        }
    }

    private void ResizeMaybe()
    {
        if (ShouldResize())
        {
            int newSize = Size * 2;
            var newKeys = new object[_keyColumns, newSize];
            var newObjects = new object[newSize];

            // var hashed = HashFunctions.Hash()
            throw new NotImplementedException();
        }
    }

    public List<KeyValuePair<List<object?>, T>> KeyValuePairs()
    {
        var result = new List<KeyValuePair<List<object?>, T>>(Size);
        for (var i = 0; i < _objects.Length; i++)
        {
            if (_objects[i] == null)
            {
                continue;
            }

            var key = new List<object?>(_keyColumns);
            for (var j = 0; j < _keyColumns; j++)
            {
                key.Add(_keys[j, i]);
            }

            var kvp = new KeyValuePair<List<object?>, T>(key, _objects[i]);
            result.Add(kvp);
        }
        return result;
    }

    private bool ShouldResize()
    {
        // resize over 70% full
        return Size > 0.7 * _objects.Length;
    }

    private bool KeysMatch(IReadOnlyList<IColumn> keys, int keyIndex, int hashIndex)
    {
        for (var c = 0; c < keys.Count; c++)
        {
            var left = _keys[c, hashIndex];
            var right = keys[c][keyIndex];

            if (left == null && right == null)
            {
                continue;
            }
            if (left == null || right == null)
            {
                return false;
            }
            if (!left.Equals(right))
            {
                return false;
            }
        }

        return true;
    }
}
