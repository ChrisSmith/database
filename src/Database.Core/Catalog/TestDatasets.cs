namespace Database.Core.Catalog;

public static class TestDatasets
{
    public static void AddTestDatasetsToCatalog(Catalog catalog)
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

        foreach (var tableName in tpchTables)
        {
            catalog.LoadTable(tableName, Path.Combine(dataPath, $"{tableName}2.parquet"));
        }

        catalog.LoadTable("table", Path.Combine(homeDir, "src/database/data.parquet"));
    }
}
