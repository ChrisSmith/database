using Database.Core.Catalog;
using Database.Core.Execution;
using DuckDB.NET.Data;

namespace Database.BenchmarkRunner;

public class DuckDbRunner : IQueryRunner
{
    private DuckDBConnection _conn;

    public TimeSpan Timeout { get; set; }

    public void Initialize()
    {
        _conn = new DuckDBConnection("Data Source=:memory:;threads=1");
        _conn.Open();

        var tables = TestDatasets.InputFiles();
        foreach (var (table, path) in tables)
        {
            using var cmd = _conn.CreateCommand();
            cmd.CommandText = $"create view {table} as select * from read_parquet('{path}');";
            cmd.ExecuteNonQuery();
        }
    }

    public string Transform(string query)
    {
        return query;
    }

    public List<Row> Run(string query, CancellationToken token)
    {
        return Run(query, token, out _);
    }

    public List<Row> Run(string query, CancellationToken token, out TimeSpan? duration)
    {
        duration = null;
        using var command = _conn.CreateCommand();
        command.CommandText = query;
        command.CommandTimeout = (int)Timeout.TotalSeconds;

        using var reader = command.ExecuteReader();

        while (reader.Read())
        {
            token.ThrowIfCancellationRequested();
            // for a fair comp I should also read all values
            // consume all rows
        }
        return [];
    }
}
