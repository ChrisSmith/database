using System.Diagnostics;
using System.Text.RegularExpressions;
using Database.Core.Execution;

namespace Database.BenchmarkRunner;

public class DataFusionRunner : IQueryRunner
{
    public void Initialize()
    {

    }

    public string Transform(string query)
    {
        // Date() function isn't supported
        query = Regex.Replace(query,
            @"(date|datetime)\s*\(([^)]+)\)\s*([\+\-])\s*interval\s*'(\d+)'\s*(year|month|day)s?",
            "$1($2, '$3$4 $5')", RegexOptions.IgnoreCase);

        return query;
    }


    // Query took 8ms and returned 1 rows
    private Regex _regex = new Regex(@"(\d+)ms", RegexOptions.Compiled);

    public List<Row> Run(string query, CancellationToken token)
    {
        return Run(query, token, out _);
    }

    public List<Row> Run(string query, CancellationToken token, out TimeSpan? duration)
    {
        duration = null;
        // TODO true FFI binding to make this cleaner
        // launch a process and process the output
        using var process = Process.Start(new ProcessStartInfo("/Users/chris/src/database/src/rust/target/release/db-rust")
        {
            UseShellExecute = false,
            CreateNoWindow = true,
            ArgumentList = { query },
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        });
        process!.Start();
        process!.WaitForExit();
        var output = process.StandardOutput.ReadToEnd();
        var matches = _regex.Matches(output);
        if (matches.Count > 0)
        {
            var milliseconds = int.Parse(matches[0].Groups[1].Value);
            duration = TimeSpan.FromMilliseconds(milliseconds);
        }

        return [];
    }

    public TimeSpan Timeout { get; set; }
}
