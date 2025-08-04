using Database.Core.Execution;

namespace Database.Core.Functions;

public class HashTable<T>
{
    public int Size { get; private set; }
    private object?[,] _keys;
    private T[] _objects;
    private bool[] _occupied;
    private int _keyColumns;

    public HashTable(int keyColumns, int size = 7)
    {
        _keyColumns = keyColumns;
        Size = 0;
        _keys = new object[keyColumns, size];
        _objects = new T[size];
        _occupied = new bool[size];
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
                    break;
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
                _keys[j, idx] = k;
            }
            Size++;
            ResizeMaybe();
        }
    }

    public T[] Get(IReadOnlyList<IColumn> keys)
    {
        var hashed = HashFunctions.Hash(keys).Values;
        var result = new T[hashed.Length];

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
                    result[i] = _objects[idx];
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
                _keys[j, idx] = k;
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
        var newKeys = new object[_keyColumns, newSize];
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
            for (var j = 0; j < _keys.GetLength(0); j++)
            {
                var k = _keys[j, i];
                newKeys[j, idx] = k!;
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
