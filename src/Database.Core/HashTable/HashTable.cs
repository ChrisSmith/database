using Database.Core.Execution;

namespace Database.Core.Functions;

public class HashTable<T>
{
    private readonly IReadOnlyList<Type> _keyTypes;
    public int Size { get; private set; }
    private List<Array> _keys;
    private T[] _objects;
    private bool[] _occupied;
    private int _keyColumns;

    public HashTable(IReadOnlyList<Type> keyTypes, int size = 7)
    {
        _keyTypes = keyTypes;
        _keyColumns = _keyTypes.Count;
        Size = 0;
        _keys = InitializeKeysArray(size);
        _objects = new T[size];
        _occupied = new bool[size];
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

    public void Add(IReadOnlyList<IColumn> keys, T[] values)
    {
        var hashed = HashFunctions.Hash(keys).Values;

        for (var i = 0; i < hashed.Length; i++)
        {
            var idx = Math.Abs(hashed[i]) % _objects.Length;
            var startPos = idx;
            while (true)
            {
                if (_occupied[idx] == false)
                {
                    Insert(idx, i);
                    break;
                }

                if (KeysMatch(keys, i, idx))
                {
                    if (ValuesMatch(_objects[idx], values[i]))
                    {
                        break;
                    }
                }

                // linear probe
                idx = (idx + 1) % _objects.Length;
                if (idx == startPos)
                {
                    throw new Exception("Hash table add failed");
                }
            }
        }

        void Insert(int idx, int i)
        {
            _objects[idx] = values[i];
            _occupied[idx] = true;
            for (var j = 0; j < keys.Count; j++)
            {
                var k = keys[j][i];
                _keys[j].SetValue(k, idx);
            }
            Size++;
            ResizeMaybe();
        }
    }

    private bool ValuesMatch(T one, T two)
    {
        return one!.Equals(two);
    }

    // Since the table allows duplicates by key,
    // we return an index of the original value, with the match
    public (List<int>, List<T>) Get(IReadOnlyList<IColumn> keys)
    {
        var hashed = HashFunctions.Hash(keys).Values;
        var result = new List<T>(hashed.Length);
        var indices = new List<int>(hashed.Length);

        for (var i = 0; i < hashed.Length; i++)
        {
            var idx = Math.Abs(hashed[i]) % _objects.Length;
            var startPos = idx;
            while (true)
            {
                if (_occupied[idx] == false)
                {
                    // miss
                    break;
                }
                if (KeysMatch(keys, i, idx))
                {
                    result.Add(_objects[idx]);
                    indices.Add(i);
                }
                // Must continue to linear probe until we hit an empty spot
                idx = (idx + 1) % _objects.Length;
                if (idx == startPos)
                {
                    throw new Exception("Hash table get failed");
                }
            }
        }

        return (indices, result);
    }

    public bool[] Contains(IReadOnlyList<IColumn> keys)
    {
        var hashed = HashFunctions.Hash(keys).Values;
        var result = new bool[hashed.Length];

        for (var i = 0; i < hashed.Length; i++)
        {
            var idx = Math.Abs(hashed[i]) % _objects.Length;
            var startPos = idx;
            while (true)
            {
                if (_occupied[idx] == false)
                {
                    // miss
                    break;
                }
                if (KeysMatch(keys, i, idx))
                {
                    result[i] = true;
                    break;
                }
                // Must continue to linear probe until we hit an empty spot
                idx = (idx + 1) % _objects.Length;
                if (idx == startPos)
                {
                    throw new Exception("Hash table get failed");
                }
            }
        }

        return result;
    }

    public List<T> GetOrAdd(IReadOnlyList<IColumn> keys, Func<T> initializer)
    {
        var hashed = HashFunctions.Hash(keys).Values;
        var result = new List<T>(hashed.Length);

        for (var i = 0; i < hashed.Length; i++)
        {
            var idx = Math.Abs(hashed[i]) % _objects.Length;
            var startPos = idx;
            while (true)
            {
                if (_occupied[idx] == false)
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
                if (idx == startPos)
                {
                    throw new Exception("Hash table get failed");
                }
            }
        }

        return result;

        void Insert(int idx, int i)
        {
            var value = initializer();
            _objects[idx] = value;
            _occupied[idx] = true;
            result.Add(value);
            for (var j = 0; j < keys.Count; j++)
            {
                var k = keys[j][i];
                _keys[j].SetValue(k, idx);
            }
            Size++;
            ResizeMaybe();
        }
    }

    private void ResizeMaybe()
    {
        if (!ShouldResize())
        {
            return;
        }

        var newSize = _objects.Length * 2;
        var newKeys = InitializeKeysArray(newSize);
        var newObjects = new T[newSize];
        var newOccupied = new bool[newSize];

        var hashed = HashFunctions.Hash(_keys, _occupied);

        for (var i = 0; i < _occupied.Length; i++)
        {
            if (!_occupied[i])
            {
                continue;
            }

            var idx = Math.Abs(hashed[i]) % newObjects.Length;
            var startPos = idx;
            while (true)
            {
                if (newOccupied[idx] == false)
                {
                    Insert(idx, i);
                    break;
                }

                // linear probe
                idx = (idx + 1) % newOccupied.Length;
                if (idx == startPos)
                {
                    throw new Exception("Hash table resize failed");
                }
            }
        }

        _keys = newKeys;
        _objects = newObjects;
        _occupied = newOccupied;

        void Insert(int idx, int i)
        {
            newObjects[idx] = _objects[i]!;
            newOccupied[idx] = true;
            for (var j = 0; j < _keys.Count; j++)
            {
                var k = _keys[j].GetValue(i);
                newKeys[j].SetValue(k!, idx);
            }
        }
    }

    public List<KeyValuePair<List<object?>, T>> KeyValuePairs()
    {
        var result = new List<KeyValuePair<List<object?>, T>>(Size);
        for (var i = 0; i < _objects.Length; i++)
        {
            if (_occupied[i] == false)
            {
                continue;
            }

            var key = new List<object?>(_keyColumns);
            for (var j = 0; j < _keyColumns; j++)
            {
                key.Add(_keys[j].GetValue(i));
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
            var left = _keys[c].GetValue(hashIndex);
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
