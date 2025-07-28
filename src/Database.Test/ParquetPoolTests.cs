using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using FluentAssertions;

namespace Database.Test;

public class ParquetPoolTests
{
    [Test]
    public void WriteToMemoryTable()
    {
        var pool = new ParquetPool();
        var memRef = pool.OpenMemoryTable();
        var table = pool.GetMemoryTable(memRef.TableId);
        table.AddColumnToSchema("foo", DataType.Int);
        table.AddColumnToSchema("bar", DataType.String);

        var colRef1 = new ColumnRef(memRef, 0, 0);
        table.PutColumn(colRef1, ColumnHelper.CreateColumn(typeof(int), "foo", new int[] { 1, 2, 3 }));

        var colRef2 = new ColumnRef(memRef, 0, 1);
        table.PutColumn(colRef2, ColumnHelper.CreateColumn(typeof(string), "bar", new string[] { "one", "two", "three" }));

        var column = table.GetColumn(colRef1);
        column.Name.Should().Be("foo");
        column.ValuesArray.Should().BeEquivalentTo(new int[] { 1, 2, 3 });

        var column2 = table.GetColumn(colRef2);
        column2.Name.Should().Be("bar");
        column2.ValuesArray.Should().BeEquivalentTo(new string[] { "one", "two", "three" });
    }
}
