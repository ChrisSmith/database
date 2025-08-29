namespace Database.Core.Catalog;

public static class TestDatasets
{
    public static List<(string, string)> InputFiles()
    {
        var homeDir = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        var dataPath = Path.Combine(homeDir, "src/database/tpch/1");

        var tpchTables = new string[]
        {
            "customer",
            "lineitem",
            "nation",
            "orders",
            "part",
            "partsupp",
            "region",
            "supplier",
        };

        return tpchTables.Select(t => (t, Path.Combine(dataPath, $"{t}2.parquet"))).ToList();
    }

    public static void AddTestDatasetsToCatalog(Catalog catalog)
    {
        foreach (var (tableName, path) in InputFiles())
        {
            catalog.LoadTable(tableName, path);
        }

        var homeDir = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        catalog.LoadTable("table", Path.Combine(homeDir, "src/database/data.parquet"));
    }
}
