using System.Diagnostics;
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
        var homeDir = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        var dataPath = Path.Combine(homeDir, "src/database/data.parquet");
        var catalog = new Catalog();
        catalog.Tables.Add(new TableSchema("table", new List<ColumnSchema>
        {
            new("Id", DataType.Int),
            new("Unordered", DataType.Int),
            new("Name", DataType.String),
        }, dataPath));
        
        var planner = new QueryPlanner(catalog);

        var sw = Stopwatch.StartNew();
        
        var scanner = new Scanner("SELECT Id, Unordered, Name FROM table;");
        var tokens = scanner.ScanTokens();
        var parser = new Parser(tokens);
        var statement = parser.Parse();

        var plan = planner.CreatePlan(statement);
        
        var it = new Interpreter();
        var result = it.Execute(plan).ToList();
        
        sw.Stop();
        Console.WriteLine($"Elapsed: {sw.ElapsedMilliseconds}ms");
        
        result.Should().HaveCount(10);
        var rg = result[0];
        rg.ColumnNames.Should().BeEquivalentTo(new List<string> { "Id", "Unordered", "Name" });
        rg.Columns.Should().HaveCount(3);
        rg.Columns[0].Should().BeOfType<Column<int>>();
        rg.Columns[1].Should().BeOfType<Column<int>>();
        rg.Columns[2].Should().BeOfType<Column<string>>();
        
        var column = (Column<int>)rg.Columns[0];
        column.Values.Should().HaveCount(10_000);
    }
}
