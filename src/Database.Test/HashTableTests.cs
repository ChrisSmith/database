using Database.Core.Execution;
using Database.Core.Functions;
using FluentAssertions;

namespace Database.Test;

public class HashTableTests
{
    [Test]
    public void Add()
    {
        var t = new HashTable<string>([typeof(int)]);
        t.Add([IntKeys([1, 2, 3])], ["one", "two", "three"]);

        var f = t.Get([IntKeys([3, 2, 1, 5])]);
        f.Should().BeEquivalentTo(["three", "two", "one", null]);
    }

    [Test]
    public void Add_No_Overwrite()
    {
        var t = new HashTable<string>([typeof(int)]);
        t.Add([IntKeys([1])], ["one"]);
        t.Add([IntKeys([1])], ["two"]);

        var f = t.Get([IntKeys([1])]);
        f.Should().BeEquivalentTo(["one"]);
        t.Size.Should().Be(1);
    }

    [Test]
    public void Add_Resizes()
    {
        var t = new HashTable<string>([typeof(int)], size: 7);
        for (var i = 0; i < 10; i++)
        {
            t.Add([IntKeys([i])], [$"{i}"]);
        }

        var f = t.Get([IntKeys([0, 1, 2, 3, 4, 5, 6, 7, 8, 9,])]);
        f.Should().BeEquivalentTo(["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]);
    }

    [Test]
    public void LoadTest()
    {
        var rg = new RowGroupRef(0);
        var rows = new RowRef?[100_000];
        var ids = new int[100_000];
        var t = new HashTable<RowRef?>([typeof(int)], size: (int)(ids.Length * 1.5));
        for (var i = 0; i < ids.Length; i++)
        {
            ids[i] = i;
            rows[i] = new RowRef(rg, i);
        }
        t.Add([IntKeys(ids)], rows);

        const int total = 100_000;
        const int batches = 10;
        const int batchSize = total / batches;
        var probeIds = new int[batchSize];
        for (var i = 0; i < batchSize; i++)
        {
            probeIds[i] = i;
        }

        for (var i = 0; i < 1; i++)
        {
            var f = t.Get([IntKeys(probeIds)]);
            f.Should().NotBeNull();
        }
    }

    private IColumn IntKeys(int[] keys)
    {
        return new Column<int>("foo", keys);
    }
}
