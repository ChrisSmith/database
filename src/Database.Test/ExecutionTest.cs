using Database.Core;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Planner;
using FluentAssertions;

namespace Database.Test;

public class ExecutionTest
{
    [Test]
    public void Test()
    {
        var scanner = new Scanner("SELECT data FROM table;");
        var tokens = scanner.ScanTokens();
        var parser = new Parser(tokens);
        var statement = parser.Parse();

        var catalog = new Catalog();
        catalog.Tables.Add(new TableSchema("table", new List<ColumnSchema>
        {
            new("data", DataType.Int),
        }, "/home/chris/src/database/example.bin"));
        
        var planner = new QueryPlanner(catalog);
        var plan = planner.CreatePlan(statement);
        
        var it = new Interpreter();
        var result = it.Execute(plan).ToList();
        result.Should().HaveCount(2560);
        var rg = result[0];
        rg.ColumnNames.Should().BeEquivalentTo(new List<string> { "data" });
        rg.Columns.Should().HaveCount(1);
        rg.Columns[0].Should().BeOfType<Column<int>>();
        var column = (Column<int>)rg.Columns[0];
        column.Values.Should().HaveCount(1024);
    }
}
