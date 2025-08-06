using Database.Core.Execution;
using Database.Core.Functions;
using FluentAssertions;

namespace Database.Test;

public class HashFunctionsTest
{
    [TestCase(1.2)]
    public void TestCollisionRate(double tableFactor)
    {
        var input = new int[100_000];
        for (var i = 0; i < input.Length; i++)
        {
            input[i] = i;
        }

        var column = ColumnHelper.CreateColumn(typeof(int), "foo", input);
        var hashed = HashFunctions.Hash([column]);

        var tableSize = (int)(input.Length * tableFactor);
        var unique = new HashSet<int>(hashed.Values.Select(v => v % tableSize));
        unique.Should().HaveCountGreaterOrEqualTo(80_000);
    }
}
