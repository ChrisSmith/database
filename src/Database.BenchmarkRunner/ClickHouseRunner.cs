using ClickHouse.Client.ADO;
using Database.Core.Catalog;
using Database.Core.Execution;

namespace Database.BenchmarkRunner;

public class ClickHouseRunner : IQueryRunner
{
    private ClickHouseConnection _conn;
    public TimeSpan Timeout { get; set; }

    public void Initialize()
    {
        var connString = "Host=localhost;Port=8123;Username=default;Password=;Database=default";
        _conn = new ClickHouseConnection(connString);
        _conn.Open();

        var tables = TestDatasets.InputFiles();
        foreach (var (table, path) in tables)
        {
            using var cmd = _conn.CreateCommand();
            cmd.CommandText = @$"CREATE OR REPLACE TABLE {table} ENGINE = File(Parquet, '{path}');";
            cmd.ExecuteNonQuery();
        }

        {

            foreach (var stmt in new[]
                     {
                         "SET max_threads = 1;",
                         "SET max_final_threads = 1;",
                         "SET background_pool_size = 1;",
                     })
            {
                using var cmd = _conn.CreateCommand();
                cmd.CommandText = stmt;
                cmd.ExecuteNonQuery();
            }
        }

    }

    public string Transform(string query)
    {
        return query;
    }

    public List<Row> Run(string query, CancellationToken token)
    {
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
