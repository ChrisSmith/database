
using System.Diagnostics;
using System.Text;
using Database.BenchmarkRunner;

Console.WriteLine("Running Benchmarks");

var runners = new IQueryRunner[]
{
    new SparkRunner(),
    new SqliteRunner(),
    new DuckDbRunner(),
    new DatabaseRunner(),
};

var timeout = TimeSpan.FromSeconds(30);
foreach (var runner in runners)
{
    runner.Timeout = timeout;
}

var queries = Enumerable.Range(1, 22).Select(i => TPCHHelpers.ReadQuery($"query_{i:00}.sql")).ToList();



var runnerNames = runners.Select(r => r.GetType().Name.Replace("Runner", "")).ToList();

var results = new Dictionary<string, (string status, long elapsedMs)>[queries.Count];
for (var i = 0; i < queries.Count; i++)
{
    results[i] = new Dictionary<string, (string status, long elapsedMs)>();
}

foreach (var runner in runners)
{
    runner.Initialize();
}

foreach (var runner in runners)
{
    var runnerName = runner.GetType().Name.Replace("Runner", "");
    Console.WriteLine($"Running {runnerName}");

    for (var i = 0; i < queries.Count; i++)
    {
        var query = queries[i];
        var queryId = i + 1;

        query = runner.Transform(query);

        var source = new CancellationTokenSource(timeout);
        var sw = Stopwatch.StartNew();

        string status;
        try
        {
            var res = runner.Run(query, source.Token);
            sw.Stop();
            status = "OK";
            Console.WriteLine($"query_{queryId:00} OK took {sw.ElapsedMilliseconds}ms");
        }
        catch (OperationCanceledException)
        {
            sw.Stop();
            status = "TIMED OUT";
            Console.WriteLine($"query_{queryId:00} TIMED OUT after {sw.ElapsedMilliseconds}ms");
        }
        catch (Exception e)
        {
            sw.Stop();
            status = "FAILED";
            Console.WriteLine($"query_{queryId:00} FAILED after {sw.ElapsedMilliseconds}ms");
            Console.WriteLine(e.Message);
        }

        results[i][runnerName] = (status, sw.ElapsedMilliseconds);
    }
}

// Generate markdown report
var markdown = new StringBuilder();
markdown.AppendLine("# Benchmark Results");
markdown.AppendLine();

// Build dynamic header
var header = new StringBuilder("| Query |");
var separator = new StringBuilder("|-------|");
var baselineRunner = runnerNames.First(); // First runner is the baseline

foreach (var runnerName in runnerNames)
{
    header.Append($" {runnerName} Status | {runnerName} Time |");
    separator.Append("---------------|---------------|");

    // Add ratio column for non-baseline runners
    if (runnerName != baselineRunner)
    {
        header.Append($" vs {baselineRunner} |");
        separator.Append("-------------|");
    }
}
markdown.AppendLine(header.ToString());
markdown.AppendLine(separator.ToString());

// Initialize stats tracking for each runner
var runnerStats = runnerNames.ToDictionary(name => name, name => new
{
    Successful = 0,
    TotalTime = 0L,
    Failures = 0,
    Timeouts = 0
});

for (int i = 0; i < queries.Count; i++)
{
    var queryName = $"query_{i + 1:00}";
    var queryLink = $"[{queryName}](Queries/{queryName}.sql)";
    var row = new StringBuilder($"| {queryLink} |");
    var baselineResult = results[i][baselineRunner];

    foreach (var runnerName in runnerNames)
    {
        var result = results[i][runnerName];

        // Format status
        string status = result.status switch
        {
            "OK" => "✅",
            "TIMED OUT" => "⏰",
            _ => "❌"
        };

        // Format time
        string time = result.status switch
        {
            "OK" => $"{result.elapsedMs:N0}ms",
            "TIMED OUT" => $"TIMEOUT ({result.elapsedMs:N0}ms)",
            _ => $"FAILED ({result.elapsedMs:N0}ms)"
        };

        row.Append($" {status} | {time} |");

        // Add ratio column for non-baseline runners
        if (runnerName != baselineRunner)
        {
            string ratio = "";
            if (result.status == "OK" && baselineResult.status == "OK" && baselineResult.elapsedMs > 0)
            {
                var multiplier = (double)result.elapsedMs / baselineResult.elapsedMs;
                if (multiplier > 1)
                {
                    ratio = $"{multiplier:F1}x slower";
                }
                else if (multiplier < 1)
                {
                    ratio = $"{(1 / multiplier):F1}x faster";
                }
                else
                {
                    ratio = "same";
                }
            }
            else if (result.status != "OK" && baselineResult.status == "OK")
            {
                ratio = result.status == "TIMED OUT" ? "TIMEOUT" : "FAILED";
            }
            else if (result.status == "OK" && baselineResult.status != "OK")
            {
                ratio = "✅ vs ❌";
            }
            else
            {
                ratio = "-";
            }

            row.Append($" {ratio} |");
        }

        // Update stats
        var stats = runnerStats[runnerName];
        if (result.status == "OK")
        {
            runnerStats[runnerName] = stats with { Successful = stats.Successful + 1, TotalTime = stats.TotalTime + result.elapsedMs };
        }
        else if (result.status == "FAILED")
        {
            runnerStats[runnerName] = stats with { Failures = stats.Failures + 1 };
        }
        else if (result.status == "TIMED OUT")
        {
            runnerStats[runnerName] = stats with { Timeouts = stats.Timeouts + 1 };
        }
    }

    markdown.AppendLine(row.ToString());
}

// Add summary
markdown.AppendLine();
markdown.AppendLine("## Summary");
markdown.AppendLine();

foreach (var runnerName in runnerNames)
{
    var stats = runnerStats[runnerName];
    var successRate = (stats.Successful / 22.0 * 100);
    markdown.AppendLine($"- **{runnerName}**: {stats.Successful}/22 queries successful ({successRate:F1}% success rate)");
    if (stats.Failures > 0) markdown.AppendLine($"  - {stats.Failures} failures");
    if (stats.Timeouts > 0) markdown.AppendLine($"  - {stats.Timeouts} timeouts");
}

// Performance comparison (only if we have multiple successful runners)
var successfulRunners = runnerStats.Where(kvp => kvp.Value.Successful > 0).ToList();
if (successfulRunners.Count > 1)
{
    markdown.AppendLine();
    markdown.AppendLine("### Performance Comparison (Successful Queries Only)");

    foreach (var (runnerName, stats) in successfulRunners)
    {
        var avgTime = stats.TotalTime / stats.Successful;
        markdown.AppendLine($"- **Average execution time ({runnerName})**: {avgTime:N0}ms");
    }

    // Calculate ratios relative to the fastest runner
    var fastestRunner = successfulRunners.MinBy(kvp => kvp.Value.TotalTime / kvp.Value.Successful);
    var fastestAvg = fastestRunner.Value.TotalTime / fastestRunner.Value.Successful;

    if (successfulRunners.Count > 1)
    {
        markdown.AppendLine();
        foreach (var (runnerName, stats) in successfulRunners)
        {
            if (runnerName != fastestRunner.Key)
            {
                var avgTime = stats.TotalTime / stats.Successful;
                var ratio = (double)avgTime / fastestAvg;
                markdown.AppendLine($"- **Performance ratio**: {runnerName} is ~{ratio:F0}x slower than {fastestRunner.Key} on average");
            }
        }
    }
}

// Write to file
await File.WriteAllTextAsync("RESULTS.md", markdown.ToString());
Console.WriteLine();
Console.WriteLine("Results written to RESULTS.md");
