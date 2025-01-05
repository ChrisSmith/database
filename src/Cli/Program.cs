using System.Diagnostics;
using Database.Core;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Planner;

var catalog = new Catalog();
catalog.Tables.Add(new TableSchema("table", new List<ColumnSchema>
{
    new("Id", DataType.Int),
    new("Unordered", DataType.Int),
    new("Name", DataType.String),
}, "/home/chris/src/database/data.parquet"));
        
var planner = new QueryPlanner(catalog);

var sw = Stopwatch.StartNew();
        
var scanner = new Scanner("SELECT Id, Unordered, Name FROM table;");
var tokens = scanner.ScanTokens();
var parser = new Parser(tokens);
var statement = parser.Parse();

var plan = planner.CreatePlan(statement);
        
var it = new Interpreter();
var result = it.Execute(plan).ToList();
var numRows = result.Sum(r => r.Columns[0].Length);
    
sw.Stop();

var rg = result[0];

var columnHeader = string.Join(", ", rg.ColumnNames);
Console.WriteLine(columnHeader);

for(var row = 0; row < 10; row++)
{
    for(var col = 0; col < rg.Columns.Count; col++)
    {
        switch (rg.Columns[col])
        {
            case Column<int> c:
                Console.Write(c.Values[row]);
                break;
            case Column<string> c:
                Console.Write(c.Values[row]);
                break;
        }
        
        if (col < rg.Columns.Count - 1)
        {
            Console.Write(", ");
        }
    }
    Console.WriteLine();
}

Console.WriteLine($"Read {numRows} rows in {sw.ElapsedMilliseconds:N}ms");
