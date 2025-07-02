using Parquet.Serialization;

const int NUM_ROWS = 100_000;

var animals = new[] { "cat", "dog", "bird", "rabbit", "fish" };

var data = new List<SimpleModel>(NUM_ROWS);
for (var i = 0; i < NUM_ROWS; i++)
{
    data.Add(new SimpleModel
    {
        Id = i,
        Name = Guid.NewGuid().ToString("D"),
        Unordered = Random.Shared.Next(10_000),
        CategoricalInt = Random.Shared.Next(5),
        CategoricalString = animals[Random.Shared.Next(animals.Length)],
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
    public int CategoricalInt { get; set; }
    public string CategoricalString { get; set; }
}
