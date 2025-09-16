
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Database.BenchmarkRunner;

// Parse command line arguments
bool runThirdParty = args.Contains("--run-third-party") || args.Contains("-t");
string resultsFile = "benchmark_results.json";

Console.WriteLine("Running Benchmarks");
if (runThirdParty)
{
    Console.WriteLine("Third-party databases will be executed and results cached");
}
else
{
    Console.WriteLine("Using cached third-party results, only running Database implementation");
}

// Always include DatabaseRunner, conditionally include third-party runners
var allRunners = new IQueryRunner[]
{
    new DuckDbRunner(),
    new ClickHouseRunner(),
    new PostgresRunner(),
    new DatabaseRunner(),
    new SparkRunner(),
    new SqliteRunner(),
};

var thirdPartyRunners = allRunners.Where(r => !(r is DatabaseRunner)).ToArray();
var databaseRunner = allRunners.First(r => r is DatabaseRunner);

var runners = runThirdParty ? allRunners : new[] { databaseRunner };

var timeout = TimeSpan.FromSeconds(30);
foreach (var runner in runners)
{
    runner.Timeout = timeout;
}

var queries = Enumerable.Range(1, 22).Select(i => TPCHHelpers.ReadQuery($"query_{i:00}.sql")).ToList();

// Load cached results if available and not running third-party
CachedResults? cachedResults = null;
if (!runThirdParty && File.Exists(resultsFile))
{
    try
    {
        var json = await File.ReadAllTextAsync(resultsFile);
        cachedResults = JsonSerializer.Deserialize<CachedResults>(json);
        Console.WriteLine("Loaded cached third-party results");
    }
    catch (Exception e)
    {
        Console.WriteLine($"Warning: Failed to load cached results: {e.Message}");
    }
}

var allRunnerNames = allRunners.Select(r => r.GetType().Name.Replace("Runner", "")).ToList();
var runnerNames = runners.Select(r => r.GetType().Name.Replace("Runner", "")).ToList();

var results = new Dictionary<string, (string status, long elapsedMs)>[queries.Count];
for (var i = 0; i < queries.Count; i++)
{
    results[i] = new Dictionary<string, (string status, long elapsedMs)>();

    // Pre-populate with cached results for third-party runners if available
    if (cachedResults != null)
    {
        foreach (var runnerName in allRunnerNames.Where(name => !runnerNames.Contains(name)))
        {
            if (cachedResults.RunnerResults.TryGetValue(runnerName, out var runnerResults) &&
                runnerResults.TryGetValue(i, out var result))
            {
                results[i][runnerName] = (result.Status, result.ElapsedMs);
            }
        }
    }
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
            if (IsKnownBadQuery(runner))
            {
                throw new OperationCanceledException($"Query {queryId} is known to fail for runner {runnerName}");
            }

            var res = runner.Run(query, source.Token);
            sw.Stop();
            status = "OK";
            var roundedMs = Math.Round(sw.ElapsedMilliseconds / 10.0) * 10;
            Console.WriteLine($"query_{queryId:00} OK took {roundedMs:N0}ms");
        }
        catch (OperationCanceledException)
        {
            sw.Stop();
            status = "TIMED OUT";
            var roundedMs = Math.Round(sw.ElapsedMilliseconds / 10.0) * 10;
            Console.WriteLine($"query_{queryId:00} TIMED OUT after {roundedMs:N0}ms");
        }
        catch (Exception e)
        {
            sw.Stop();
            status = "FAILED";
            var roundedMs = Math.Round(sw.ElapsedMilliseconds / 10.0) * 10;
            Console.WriteLine($"query_{queryId:00} FAILED after {roundedMs:N0}ms");
            Console.WriteLine(e.Message);
        }

        results[i][runnerName] = (status, sw.ElapsedMilliseconds);

        bool IsKnownBadQuery(IQueryRunner queryRunner)
        {
            if (queryRunner is SqliteRunner)
            {
                var ids = new[] { 4, 5, 17, 18, 19, 20, 21, 22 };
                return ids.Contains(queryId);
            }
            return false;
        }
    }
}

// Save results to cache if we ran third-party databases
if (runThirdParty)
{
    try
    {
        var cacheData = new CachedResults(
            allRunnerNames.ToDictionary(
                runnerName => runnerName,
                runnerName => Enumerable.Range(0, queries.Count)
                    .Where(i => results[i].ContainsKey(runnerName))
                    .ToDictionary(
                        i => i,
                        i => new BenchmarkResult(results[i][runnerName].status, results[i][runnerName].elapsedMs)
                    )
            )
        );

        var json = JsonSerializer.Serialize(cacheData, new JsonSerializerOptions { WriteIndented = true });
        await File.WriteAllTextAsync(resultsFile, json);
        Console.WriteLine($"Results cached to {resultsFile}");
    }
    catch (Exception e)
    {
        Console.WriteLine($"Warning: Failed to save results cache: {e.Message}");
    }
}

// Generate markdown report
var markdown = new StringBuilder();
markdown.AppendLine("# Benchmark Results");
markdown.AppendLine();

// Build dynamic header with queries as columns
var header = new StringBuilder("| Runner |");
var separator = new StringBuilder("|--------|");

for (int i = 0; i < queries.Count; i++)
{
    var queryName = $"query_{i + 1:00}";
    var queryLink = $"[{queryName}](Queries/{queryName}.sql)";
    header.Append($" {queryLink} |");
    separator.Append("------------|");
}
markdown.AppendLine(header.ToString());
markdown.AppendLine(separator.ToString());

// Initialize stats tracking for each runner (including cached ones)
var reportRunnerNames = allRunnerNames;
var runnerStats = reportRunnerNames.ToDictionary(name => name, name => new
{
    Successful = 0,
    TotalTime = 0L,
    Failures = 0,
    Timeouts = 0
});

// Generate rows for each runner
var baselineRunner = reportRunnerNames.First(); // First runner is the baseline

foreach (var runnerName in reportRunnerNames)
{
    var row = new StringBuilder($"| **{runnerName}** |");

    for (int i = 0; i < queries.Count; i++)
    {
        if (!results[i].TryGetValue(runnerName, out var result))
        {
            // No result available for this runner/query combination
            row.Append(" N/A |");
            continue;
        }

        // Format status and time combined
        var roundedMs = Math.Round(result.elapsedMs / 10.0) * 10;
        string cellContent = result.status switch
        {
            "OK" => $"{roundedMs:N0}ms",
            _ => "❌"
        };

        // Add ratio for non-baseline runners
        if (runnerName != baselineRunner && result.status == "OK")
        {
            if (results[i].TryGetValue(baselineRunner, out var baselineResult) &&
                baselineResult.status == "OK" && baselineResult.elapsedMs > 0)
            {
                var multiplier = (double)result.elapsedMs / baselineResult.elapsedMs;
                if (multiplier > 1)
                {
                    cellContent += $" ({multiplier:F1}x)";
                }
                else if (multiplier < 1)
                {
                    cellContent += $" ({(1 / multiplier):F1}x)";
                }
                // Don't show anything for "same" performance to keep it clean
            }
        }

        row.Append($" {cellContent} |");

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

foreach (var runnerName in reportRunnerNames)
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

// Define serializable result structure
public record BenchmarkResult(string Status, long ElapsedMs);
public record CachedResults(Dictionary<string, Dictionary<int, BenchmarkResult>> RunnerResults);
