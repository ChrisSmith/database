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

var previousLines = new List<string>();

while (true)
{
    try
    {
        Console.Write("> ");
        var query = Console.ReadLine();
        if (string.IsNullOrWhiteSpace(query))
        {
            continue;
        }
        if (query == ".exit")
        {
            break;
        }
        previousLines.Add(query);

        var sw = Stopwatch.StartNew();

        var scanner = new Scanner(query);
        var tokens = scanner.ScanTokens();
        var parser = new Parser(tokens);
        var statement = parser.Parse();

        var plan = planner.CreatePlan(statement);

        var it = new Interpreter();
        var result = it.Execute(plan).ToList();
        sw.Stop();

        PrintTable(result);

        var numRows = result.Sum(r => r.Columns[0].Length);
        Console.WriteLine($"{numRows} rows in {sw.ElapsedMilliseconds:N}ms");
    }
    catch (Exception ex)
    {
        Console.WriteLine("Error: " + ex.Message);
        Console.WriteLine(ex);
    }
}

void PrintTable(List<RowGroup> result)
{
    var rg = result[0];
    var columnHeader = string.Join(", ", rg.Columns.Select(c => c.Name));
    Console.WriteLine(columnHeader);

    var max = Math.Min(10, rg.Columns[0].Length);

    for (var row = 0; row < max; row++)
    {
        for (var col = 0; col < rg.Columns.Count; col++)
        {
            switch (rg.Columns[col])
            {
                case Column<int> c:
                    Console.Write(c.Values[row]);
                    break;
                case Column<double> c:
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
}



