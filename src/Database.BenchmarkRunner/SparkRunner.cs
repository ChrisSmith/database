using System.Data.Odbc;
using System.Text.RegularExpressions;
using Database.Core.Catalog;
using Database.Core.Execution;
using DuckDB.NET.Data;
using Microsoft.Data.Sqlite;

namespace Database.BenchmarkRunner;

public class SparkRunner : IQueryRunner
{
    private OdbcConnection _conn;
    public TimeSpan Timeout { get; set; }

    public void Initialize()
    {
        string connStr = "DSN=SparkODBC;UID=;PWD=;";
        _conn = new OdbcConnection(connStr);
        _conn.Open();

        var tables = TestDatasets.InputFiles();
        foreach (var (table, path) in tables)
        {
            using var cmd = _conn.CreateCommand();
            cmd.CommandText = @$"CREATE OR REPLACE TEMPORARY VIEW {table} USING parquet OPTIONS (path '{path}');";
            cmd.ExecuteNonQuery();
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
