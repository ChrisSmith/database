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
        // (A1) Function form DATE(...) +/- INTERVAL
        query = Regex.Replace(query,
            @"(date|datetime)\s*\(([^)]+)\)\s*([\+\-])\s*interval\s*'(\d+)'\s*(year|month|day)s?",
            "$1($2, '$3$4 $5')", RegexOptions.IgnoreCase);

        // (A2) DATE '...' +/- INTERVAL
        query = Regex.Replace(query,
            @"date\s*'([^']+)'\s*([\+\-])\s*interval\s*'(\d+)'\s*(year|month|day)s?",
            "date('$1', '$2$3 $4')", RegexOptions.IgnoreCase);

        // (A3) Standalone DATE 'YYYY-MM-DD'
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

        // AS c_orders (c_custkey, c_count)
        if (query.Contains("AS c_orders (c_custkey, c_count)"))
        {
            query = query.Replace("AS c_orders (c_custkey, c_count)", "AS c_orders");
            query = query.Replace("COUNT(o_orderkey)", "COUNT(o_orderkey) AS c_count");
        }

        // SUBSTRING(expr FROM start FOR length) → substr(expr, start, length)
        query = Regex.Replace(query,
            @"substring\s*\(\s*([^\s]+)\s+from\s+(\d+)\s+for\s+(\d+)\s*\)",
            "substr($1, $2, $3)",
            RegexOptions.IgnoreCase);

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
