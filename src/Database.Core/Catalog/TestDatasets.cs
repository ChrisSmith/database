namespace Database.Core.Catalog;

public static class TestDatasets
{
    public static void AddTestDatasetsToCatalog(Catalog catalog)
    {
        var homeDir = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        var dataPath = Path.Combine(homeDir, "src/database/tpch/1");

        var tableName = "lineitem";
        catalog.LoadTable(tableName, Path.Combine(dataPath, $"{tableName}.parquet"));


        catalog.LoadTable("table", Path.Combine(homeDir, "src/database/data.parquet"));
    }
}
