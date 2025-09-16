using Database.Core.Catalog;
using Database.Core.Execution;
using Npgsql;

namespace Database.BenchmarkRunner;

public class PostgresRunner : IQueryRunner
{
    private NpgsqlConnection _conn;

    public TimeSpan Timeout { get; set; }

    public void Initialize()
    {
        _conn = NpgsqlDataSource.Create("Host=localhost;Username=chris;Password=;Database=postgres").OpenConnection();

        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SET max_parallel_workers_per_gather = 0;";
        cmd.ExecuteNonQuery();
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

