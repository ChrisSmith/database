using System.Diagnostics;
using Database.Core;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Planner;

var catalog = new Catalog();

var homeDir = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
var dataPath = Path.Combine(homeDir, "src/database/data.parquet");

catalog.Tables.Add(new TableSchema("table", new List<ColumnSchema>
{
    new("Id", DataType.Int, typeof(int)),
    new("Unordered", DataType.Int, typeof(int)),
    new("Name", DataType.String, typeof(string)),
    new("CategoricalInt", DataType.Int, typeof(int)),
    new("CategoricalString", DataType.String, typeof(string))
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
var numRows = result.Sum(r => r.Columns[0].Length);

sw.Stop();

var rg = result[0];

var columnHeader = string.Join(", ", rg.Schema.Columns.Select(c => c.Name));
Console.WriteLine(columnHeader);

for (var row = 0; row < 10; row++)
{
    for (var col = 0; col < rg.Columns.Count; col++)
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

// print process memory usage
var proc = Process.GetCurrentProcess();
Console.WriteLine($"Working Set: {proc.WorkingSet64 / 1024 / 1024}MB Private: {proc.PrivateMemorySize64 / 1024 / 1024}MB\n");
Console.WriteLine($"Read {numRows} rows in {sw.ElapsedMilliseconds:N}ms");

