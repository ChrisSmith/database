using Database.Core.Execution;
using Database.Core.Functions;
using FluentAssertions;

namespace Database.Test;

public class HashTableTests
{
    [Test]
    public void Add()
    {
        var t = new HashTable<string>(1);
        t.Add([IntKeys([1, 2, 3])], ["one", "two", "three"]);

        var f = t.Get([IntKeys([3, 2, 1, 5])]);
        f.Should().BeEquivalentTo(["three", "two", "one", null]);
    }

    [Test]
    public void Add_No_Overwrite()
    {
        var t = new HashTable<string>(1);
        t.Add([IntKeys([1])], ["one"]);
        t.Add([IntKeys([1])], ["two"]);

        var f = t.Get([IntKeys([1])]);
        f.Should().BeEquivalentTo(["one"]);
        t.Size.Should().Be(1);
    }

    [Test]
    public void Add_Resizes()
    {
        var t = new HashTable<string>(1, size: 7);
        for (var i = 0; i < 10; i++)
        {
            t.Add([IntKeys([i])], [$"{i}"]);
        }

        var f = t.Get([IntKeys([0, 1, 2, 3, 4, 5, 6, 7, 8, 9,])]);
        f.Should().BeEquivalentTo(["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]);
    }

    private IColumn IntKeys(int[] keys)
    {
        return new Column<int>("foo", keys);
    }
}
