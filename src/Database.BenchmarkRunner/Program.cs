
using System.Diagnostics;
using Database.BenchmarkRunner;

Console.WriteLine("Running Benchmarks");

var runners = new IQueryRunner[] { new DuckDbRunner(), new DatabaseRunner() };

var queries = Enumerable.Range(1, 22).Select(i => TPCHHelpers.ReadQuery($"query_{i:00}.sql")).ToList();

foreach (var runner in runners)
{
    runner.Initialize();
}

foreach (var runner in runners)
{
    Console.WriteLine($"Running {runner.GetType().Name}");

    for (var i = 0; i < queries.Count; i++)
    {
        var query = queries[i];
        var queryId = i + 1;
        var source = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        var sw = Stopwatch.StartNew();
        try
        {
            var res = runner.Run(query, source.Token);
            sw.Stop();
            Console.WriteLine($"query_{queryId:00} OK took {sw.ElapsedMilliseconds}ms");
        }
        catch (OperationCanceledException)
        {
            sw.Stop();
            Console.WriteLine($"query_{queryId:00} TIMED OUT after {sw.ElapsedMilliseconds}ms");
        }
        catch (Exception e)
        {
            sw.Stop();
            Console.WriteLine($"query_{queryId:00} FAILED after {sw.ElapsedMilliseconds}ms");
        }
    }
}
