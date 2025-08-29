using System.Text.RegularExpressions;
using Database.Core.Catalog;
using Database.Core.Execution;
using DuckDB.NET.Data;
using Microsoft.Data.Sqlite;

namespace Database.BenchmarkRunner;

public class SqliteRunner : IQueryRunner
{
    private SqliteConnection _conn;

    public TimeSpan Timeout { get; set; }

    public void Initialize()
    {
        _conn = new SqliteConnection("Data Source=/Users/chris/src/database/sqlite_database.db");
        _conn.Open();
    }

    public string Transform(string query)
    {
        // 1. DATE literal +/- INTERVAL (must come first!)
        query = Regex.Replace(query,
            @"date\s*'([^']+)'\s*([\+\-])\s*interval\s*'(\d+)'\s*(year|month|day)s?",
            "date('$1', '$2$3 $4')",
            RegexOptions.IgnoreCase);

        // 2. Plain DATE literal
        query = Regex.Replace(query,
            @"date\s*'([^']+)'",
            "date('$1')",
            RegexOptions.IgnoreCase);

        // 3. Casts: strip DECIMAL/NUMERIC precision
        query = Regex.Replace(query,
            @"cast\(([^)]+)\s+as\s+decimal\([^)]*\)\)",
            "$1",
            RegexOptions.IgnoreCase);
        query = Regex.Replace(query,
            @"cast\(([^)]+)\s+as\s+numeric\([^)]*\)\)",
            "$1",
            RegexOptions.IgnoreCase);

        // 4. EXTRACT → strftime
        query = Regex.Replace(query,
            @"extract\s*\(\s*year\s*from\s*([^)]+)\)",
            "strftime('%Y', $1)",
            RegexOptions.IgnoreCase);
        query = Regex.Replace(query,
            @"extract\s*\(\s*month\s*from\s*([^)]+)\)",
            "strftime('%m', $1)",
            RegexOptions.IgnoreCase);
        query = Regex.Replace(query,
            @"extract\s*\(\s*day\s*from\s*([^)]+)\)",
            "strftime('%d', $1)",
            RegexOptions.IgnoreCase);

        // 5. Force float math for discount/tax
        query = query.Replace("1 - l_discount", "1.0 - l_discount")
            .Replace("1 + l_tax", "1.0 + l_tax");

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
