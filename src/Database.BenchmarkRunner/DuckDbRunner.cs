using Database.Core.Catalog;
using Database.Core.Execution;
using DuckDB.NET.Data;

namespace Database.BenchmarkRunner;

public class DuckDbRunner : IQueryRunner
{
    private DuckDBConnection _conn;


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

    public List<Row> Run(string query, CancellationToken token)
    {
        using var command = _conn.CreateCommand();
        command.CommandText = query;

        using var reader = command.ExecuteReader();

        while (reader.Read())
        {
            // for a fair comp I should also read all values
            // consume all rows
        }
        return [];
    }
}
