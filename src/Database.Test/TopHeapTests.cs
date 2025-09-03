using Database.Core.Execution;
using Database.Core.Functions;
using FluentAssertions;

namespace Database.Test;

public class TopHeapTests
{
    [TestCase(true)]
    [TestCase(false)]
    public void Add(bool reverse)
    {
        var keys = OrderedKeys(8);
        var objects = OrderedRefs(8);

        if (reverse)
        {
            keys = keys.Reverse().ToArray();
            objects = objects.Reverse().ToArray();
        }

        var top = new TopHeap<RowRef>([typeof(int)], 3);
        top.Insert([keys], objects);
        top.Size.Should().Be(3);

        var values = top.ToArray();
        values.Should().BeEquivalentTo(OrderedRefs(3));

        var foo = top.GetKeys(0).ToArray();
        foo.Should().BeEquivalentTo(OrderedKeys(3));
    }

    [Test]
    public void Duplicates()
    {
        var keys = OrderedKeys(8).Concat(OrderedKeys(8)).ToArray();
        var objects = OrderedRefs(8).Concat(OrderedRefs(8)).ToArray();

        var top = new TopHeap<RowRef>([typeof(int)], 3);
        top.Insert([keys], objects);
        top.Size.Should().Be(3);

        var values = top.ToArray();
        values.Should().BeEquivalentTo(new[]
        {
            new RowRef(default, 0),
            new RowRef(default, 0),
            new RowRef(default, 1),
        });

        var foo = top.GetKeys(0).ToArray();
        foo.Should().BeEquivalentTo(new[] { 0, 0, 1 });
    }

    [TestCase(true)]
    [TestCase(false)]
    public void AddTwoKeys(bool reverse)
    {
        var key1 = Enumerable.Range(0, 8).Select(k => k / 2).ToArray();
        var key2 = Enumerable.Range(0, 8).Select(k => k % 2).ToArray();
        var objects = OrderedRefs(8);

        if (reverse)
        {
            key1 = key1.Reverse().ToArray();
            key2 = key2.Reverse().ToArray();
            objects = objects.Reverse().ToArray();
        }

        var top = new TopHeap<RowRef>([typeof(int), typeof(int)], 5);
        top.Insert([key1, key2], objects);
        top.Size.Should().Be(5);

        var values = top.ToArray();
        values.Should().BeEquivalentTo(new[]
        {
            new RowRef(default, 0),
            new RowRef(default, 1),
            new RowRef(default, 2),
            new RowRef(default, 3),
            new RowRef(default, 4),
        });

        var resKey1 = top.GetKeys(0).ToArray();
        resKey1.Should().BeEquivalentTo(new[] { 0, 0, 1, 1, 2 });

        var resKey2 = top.GetKeys(1).ToArray();
        resKey2.Should().BeEquivalentTo(new[] { 0, 1, 0, 1, 0 });
    }

    private int[] OrderedKeys(int size)
    {
        var result = new int[size];
        for (var i = 0; i < size; i++)
        {
            result[i] = i;
        }
        return result;
    }

    private RowRef[] OrderedRefs(int size)
    {
        var result = new RowRef[size];
        for (var i = 0; i < size; i++)
        {
            result[i] = new(default, i);
        }
        return result;
    }

}
