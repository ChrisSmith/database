using Parquet.Serialization;

const int NUM_ROWS = 100_000;

var data = new List<SimpleModel>(NUM_ROWS);
for (var i = 0; i < NUM_ROWS; i++)
{
    data.Add(new SimpleModel
    {
        Id = i,
        Name = Guid.NewGuid().ToString("D"),
        Unordered = Random.Shared.Next(10_000),
    });
}

await ParquetSerializer.SerializeAsync(data, "data.parquet", new ParquetSerializerOptions()
{
    RowGroupSize = 10_000,
});

Console.WriteLine("Done");

public record SimpleModel
{
    public int Id { get; set; }

    public int Unordered { get; set; }
    public string Name { get; set; }
}
