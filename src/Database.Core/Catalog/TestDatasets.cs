namespace Database.Core.Catalog;

public static class TestDatasets
{
    public static void AddTestDatasetsToCatalog(Catalog catalog)
    {
        var homeDir = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        var dataPath = Path.Combine(homeDir, "src/database/tpch/1");
        var tableName = "lineitem";

        var colId = 1;
        var tableId = 1;

        catalog.Tables.Add(new TableSchema((TableId)tableId++, tableName, new List<ColumnSchema>
        {
            new((ColumnId)colId++, "l_orderkey", DataType.Long, typeof(long)),
            new((ColumnId)colId++, "l_partkey", DataType.Int, typeof(int)),
            new((ColumnId)colId++, "l_suppkey", DataType.Int, typeof(int)),
            new((ColumnId)colId++, "l_linenumber", DataType.Int, typeof(int)),
            new((ColumnId)colId++, "l_quantity", DataType.Double, typeof(double)),
            new((ColumnId)colId++, "l_extendedprice", DataType.Double, typeof(double)),
            new((ColumnId)colId++, "l_discount", DataType.Double, typeof(double)),
            new((ColumnId)colId++, "l_tax", DataType.Double, typeof(double)),
            new((ColumnId)colId++, "l_returnflag", DataType.String, typeof(string)),
            new((ColumnId)colId++, "l_linestatus", DataType.String, typeof(string)),
            new((ColumnId)colId++, "l_shipdate", DataType.DateTime, typeof(DateTime)),
            new((ColumnId)colId++, "l_commitdate", DataType.DateTime, typeof(DateTime)),
            new((ColumnId)colId++, "l_receiptdate", DataType.DateTime, typeof(DateTime)),
            new((ColumnId)colId++, "l_shipinstruct", DataType.String, typeof(string)),
            new((ColumnId)colId++, "l_shipmode", DataType.String, typeof(string)),
            new((ColumnId)colId++, "l_comment", DataType.String, typeof(string)),
        }, Path.Join(dataPath, $"{tableName}.parquet")));

        catalog.Tables.Add(new TableSchema((TableId)tableId++, "table", new List<ColumnSchema>
        {
            new((ColumnId)colId++, "Id", DataType.Int, typeof(int)),
            new((ColumnId)colId++, "Unordered", DataType.Int, typeof(int)),
            new((ColumnId)colId++, "Name", DataType.String, typeof(string)),
            new((ColumnId)colId++, "CategoricalInt", DataType.Int, typeof(int)),
            new((ColumnId)colId++, "CategoricalString", DataType.String, typeof(string)),
        }, Path.Combine(homeDir, "src/database/data.parquet")));

    }
}
