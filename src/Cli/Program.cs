using System.Diagnostics;
using Database.Core;
using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Planner;

var bufferPool = new ParquetPool();
var catalog = new Catalog(bufferPool);
TestDatasets.AddTestDatasetsToCatalog(catalog);

var planner = new QueryPlanner(catalog, bufferPool);

var previousLines = new List<string>();

// Load history from ~/.cache/databasecli.txt
var cacheDir = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".cache");
var historyPath = Path.Combine(cacheDir, "databasecli.txt");
if (File.Exists(historyPath))
{
    previousLines = File.ReadAllLines(historyPath).TakeLast(100).ToList();
}
Directory.CreateDirectory(cacheDir);

if (args.Length > 0)
{
    var query = args[0];
    EvalQuery(query, planner);
    return;
}


while (true)
{
    try
    {
        Console.Write("> ");
        var query = ReadLineWithHistory(previousLines);
        if (string.IsNullOrWhiteSpace(query))
        {
            continue;
        }
        if (query == ".exit")
        {
            break;
        }
        previousLines.Add(query);

        if (previousLines.Count > 100)
        {
            previousLines.RemoveAt(0);
        }
        File.WriteAllLines(historyPath, previousLines);

        EvalQuery(query, planner);
    }
    catch (Exception ex)
    {
        Console.WriteLine("Error: " + ex.Message);
        Console.WriteLine(ex);
    }
}

string ReadLineWithHistory(List<string> previousLines)
{
    var input = new List<char>();
    var historyIndex = previousLines.Count;
    var currentInput = "";
    int cursorPos = 0;
    int prevRenderLength = 0;

    void RenderInput()
    {
        int clearLength = Math.Max(prevRenderLength, input.Count);
        Console.Write("\r> " + new string(' ', clearLength));
        Console.Write("\r> " + new string(input.ToArray()));
        int endPos = input.Count;
        if (cursorPos < endPos)
        {
            int moveLeft = endPos - cursorPos;
            if (moveLeft > 0)
            {
                Console.Write($"\u001b[{moveLeft}D");
            }
        }
        prevRenderLength = input.Count;
    }

    while (true)
    {
        var keyInfo = Console.ReadKey(intercept: true);

        if (keyInfo.Key == ConsoleKey.Enter)
        {
            Console.WriteLine();
            break;
        }
        else if (keyInfo.Key == ConsoleKey.UpArrow)
        {
            if (previousLines.Count > 0 && historyIndex > 0)
            {
                historyIndex--;
                input = previousLines[historyIndex].ToList();
                cursorPos = input.Count;
                currentInput = new string(input.ToArray());
                RenderInput();
            }
        }
        else if (keyInfo.Key == ConsoleKey.DownArrow)
        {
            if (previousLines.Count > 0 && historyIndex < previousLines.Count - 1)
            {
                historyIndex++;
                input = previousLines[historyIndex].ToList();
                cursorPos = input.Count;
                currentInput = new string(input.ToArray());
                RenderInput();
            }
            else if (historyIndex == previousLines.Count - 1)
            {
                historyIndex++;
                input = new List<char>();
                cursorPos = 0;
                currentInput = "";
                RenderInput();
            }
        }
        else if (keyInfo.Key == ConsoleKey.Backspace)
        {
            if (cursorPos > 0)
            {
                input.RemoveAt(cursorPos - 1);
                cursorPos--;
                currentInput = new string(input.ToArray());
                RenderInput();
            }
        }
        else if (keyInfo.Key == ConsoleKey.Delete)
        {
            if (cursorPos < input.Count)
            {
                input.RemoveAt(cursorPos);
                currentInput = new string(input.ToArray());
                RenderInput();
            }
        }
        else if (keyInfo.Key == ConsoleKey.LeftArrow)
        {
            if (cursorPos > 0)
            {
                cursorPos--;
                RenderInput();
            }
        }
        else if (keyInfo.Key == ConsoleKey.RightArrow)
        {
            if (cursorPos < input.Count)
            {
                cursorPos++;
                RenderInput();
            }
        }
        else if (keyInfo.Key == ConsoleKey.Home)
        {
            if (cursorPos != 0)
            {
                cursorPos = 0;
                RenderInput();
            }
        }
        else if (keyInfo.Key == ConsoleKey.End)
        {
            if (cursorPos != input.Count)
            {
                cursorPos = input.Count;
                RenderInput();
            }
        }
        else if (!char.IsControl(keyInfo.KeyChar))
        {
            input.Insert(cursorPos, keyInfo.KeyChar);
            cursorPos++;
            currentInput = new string(input.ToArray());
            RenderInput();
        }
    }
    return new string(input.ToArray());
}

void EvalQuery(string query, QueryPlanner queryPlanner)
{
    var stopwatch = Stopwatch.StartNew();
    var scanner = new Scanner(query);
    var tokens = scanner.ScanTokens();
    var parser = new Parser(tokens);
    var statement = parser.Parse();

    var plan = queryPlanner.CreatePlan(statement);

    var it = new Interpreter(bufferPool);
    var result = it.Execute(plan).ToList();
    stopwatch.Stop();

    PrintTable(result);

    var numRows = result.Sum(r => r.Columns[0].Length);
    Console.WriteLine($"{numRows} rows in {stopwatch.ElapsedMilliseconds:N}ms");
}

void PrintTable(List<MaterializedRowGroup> result)
{
    if (result.Count == 0)
    {
        Console.WriteLine("No rows returned");
        return;
    }
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



