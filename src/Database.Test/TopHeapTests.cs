using Database.Core.Execution;
using Database.Core.Functions;
using FluentAssertions;
using static Database.Core.Functions.SortOrder;
namespace Database.Test;

public class TopHeapTests
{
    [Test]
    public void Add(
        [Values(true, false)] bool reverse,
        [Values(Ascending, Descending)] SortOrder sortOrder)
    {
        var keys = OrderedKeys(8);
        var objects = OrderedRefs(8);

        var expected = sortOrder == Ascending
            ? objects.Take(3).ToArray()
            : objects.Reverse().Take(3).ToArray();

        var expectedKeys = sortOrder == Ascending
            ? keys.Take(3).ToArray()
            : keys.Reverse().Take(3).ToArray();

        if (reverse)
        {
            keys = keys.Reverse().ToArray();
            objects = objects.Reverse().ToArray();
        }

        var top = new TopHeap<RowRef>([typeof(int)], new[] { sortOrder, }, 3);
        top.Insert([keys], objects);
        top.Size.Should().Be(3);

        var values = top.ToArray();
        values.Should().BeEquivalentTo(expected);

        var foo = top.GetKeys(0).ToArray();
        foo.Should().BeEquivalentTo(expectedKeys);
    }

    [Test]
    public void Duplicates()
    {
        var keys = OrderedKeys(8).Concat(OrderedKeys(8)).ToArray();
        var objects = OrderedRefs(8).Concat(OrderedRefs(8)).ToArray();

        var top = new TopHeap<RowRef>([typeof(int)], new[] { Ascending }, 3);
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

        var top = new TopHeap<RowRef>([typeof(int), typeof(int)], new[] { Ascending, Ascending }, 5);
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

    [Test]
    public void LongSequences(
        [Range(1, 10)] int T,
        [Values(true, false)] bool reverse,
        [Values(Ascending, Descending)] SortOrder sortOrder1,
        [Values(Ascending, Descending)] SortOrder sortOrder2
        )
    {
        const int N = 10;
        var key1 = new int[N];
        for (var i = 0; i < N; i++)
        {
            key1[i] = i / 3;
        }

        var key2 = new int[N];
        for (var i = 0; i < N; i++)
        {
            key2[i] = i % 3;
        }

        var objects = OrderedRefs(N);

        var expected = ((sortOrder1, sortOrder2) switch
        {
            (Ascending, Ascending) => objects,
            (Descending, Descending) => objects.Reverse(),
            (Ascending, Descending) =>
            [
                M(2), M(1), M(0),
                M(5), M(4), M(3),
                M(8), M(7), M(6),
                M(9),
            ],
            (Descending, Ascending) => [
                M(9),
                M(6), M(7), M(8),
                M(3), M(4), M(5),
                M(0), M(1), M(2),
            ],
            _ => throw new ArgumentOutOfRangeException()
        }).Take(T).ToArray();

        if (reverse)
        {
            key1 = key1.Reverse().ToArray();
            key2 = key2.Reverse().ToArray();
            objects = objects.Reverse().ToArray();
        }

        var top = new TopHeap<RowRef>([typeof(int), typeof(int)], [sortOrder1, sortOrder2], T);
        top.Insert([key1, key2], objects);
        top.Size.Should().Be(T);

        var values = top.ToArray();
        values.Should().BeEquivalentTo(expected);

        var seq1 = new[] { 0, 0, 0, 1, 1, 1, 2, 2, 2, 3, };
        var seq2 = new[] { 0, 1, 2, 0, 1, 2, 0, 1, 2, 0, };

        var alt1 = new[] { 0, 0, 0, 1, 1, 1, 2, 2, 2, 3, };
        var alt2 = new[] { 2, 1, 0, 2, 1, 0, 2, 1, 0, 0, };

        (seq1, seq2) = (sortOrder1, sortOrder2) switch
        {
            (Ascending, Ascending) => (seq1, seq2),
            (Descending, Descending) => (seq1.Reverse().ToArray(), seq2.Reverse().ToArray()),
            (Ascending, Descending) => (alt1, alt2),
            (Descending, Ascending) => (alt1.Reverse().ToArray(), alt2.Reverse().ToArray()),
            _ => throw new ArgumentOutOfRangeException()
        };

        seq1 = seq1.Take(T).ToArray();
        seq2 = seq2.Take(T).ToArray();

        var resKey1 = top.GetKeys(0).ToArray();
        resKey1.Should().BeEquivalentTo(seq1.Take(T));

        var resKey2 = top.GetKeys(1).ToArray();
        resKey2.Should().BeEquivalentTo(seq2.Take(T));
        return;

        RowRef M(int i) => new(default, i);
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
