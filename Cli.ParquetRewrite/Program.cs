
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using Parquet.Serialization;

var homeDir = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
var inputPath = Path.Combine(homeDir, "src/database/tpch/1/lineitem.parquet");
var outputPath = Path.Combine(homeDir, "src/database/tpch/1/lineitem2.parquet");

if (File.Exists(outputPath))
{
    File.Delete(outputPath);
}

using var reader = await ParquetReader.CreateAsync(inputPath);

PrintSchemaFields(reader.Schema);

var updatedSchema = new ParquetSchema(reader.Schema.Fields.Select(MakeNotNull).ToList());

PrintSchemaFields(updatedSchema);

await using var writer = await ParquetWriter.CreateAsync(updatedSchema, File.OpenWrite(outputPath), formatOptions: null);

// Copy each rowgroup
for (int rgIndex = 0; rgIndex < reader.RowGroupCount; rgIndex++)
{
    using var rowGroupReader = reader.OpenRowGroupReader(rgIndex);
    using var rowGroup = writer.CreateRowGroup();

    // Copy each column in the rowgroup
    for (int colIndex = 0; colIndex < reader.Schema.DataFields.Length; colIndex++)
    {
        var orgField = reader.Schema.DataFields[colIndex];
        var newField = updatedSchema.DataFields[colIndex];

        var oldColumn = await rowGroupReader.ReadColumnAsync(orgField);
        var newColumn = MakeColumnNotNull(newField, oldColumn);
        await rowGroup.WriteColumnAsync(newColumn);
    }
}
Console.WriteLine("Done");

return;

DataColumn MakeColumnNotNull(DataField newField, DataColumn orgColumn)
{
    Array finalCopy = orgColumn.Data;
    if (orgColumn.Data is decimal?[] dec)
    {
        var copy = new decimal[orgColumn.Data.Length];
        for (var j = 0; j < orgColumn.Data.Length && j < dec.Length; j++)
        {
            copy[j] = (decimal)dec[j]!;
        }
        finalCopy = copy;
    }
    if (orgColumn.Data is double?[] dou)
    {
        var copy = new double[orgColumn.Data.Length];
        for (var j = 0; j < orgColumn.Data.Length && j < dou.Length; j++)
        {
            copy[j] = (double)dou[j]!;
        }
        finalCopy = copy;
    }
    else if (orgColumn.Data is long?[] decl)
    {
        var copy = new long[orgColumn.Data.Length];
        for (var j = 0; j < orgColumn.Data.Length && j < decl.Length; j++)
        {
            copy[j] = (long)decl[j]!;
        }
        finalCopy = copy;
    }
    else if (orgColumn.Data is int?[] deci)
    {
        var copy = new int[orgColumn.Data.Length];
        for (var j = 0; j < orgColumn.Data.Length && j < deci.Length; j++)
        {
            copy[j] = (int)deci[j]!;
        }
        finalCopy = copy;
    }
    else if (orgColumn.Data is DateTime?[] decdt)
    {
        var copy = new DateTime[orgColumn.Data.Length];
        for (var j = 0; j < orgColumn.Data.Length && j < decdt.Length; j++)
        {
            copy[j] = (DateTime)decdt[j]!;
        }
        finalCopy = copy;
    }

    var newColumn = new DataColumn(newField, finalCopy);
    return newColumn;
}

Field MakeNotNull(Field field)
{
    if (field is not DataField d)
    {
        return field;
    }

    return field switch
    {
        DecimalDataField dd => new DecimalDataField(d.Name, dd.Precision, dd.Scale, isNullable: false, isArray: d.IsArray),
        DateTimeDataField dt => new DateTimeDataField(dt.Name, dt.DateTimeFormat, dt.IsAdjustedToUTC, dt.Unit, isNullable: false, isArray: d.IsArray),
        TimeOnlyDataField to => new TimeOnlyDataField(to.Name, to.TimeSpanFormat, isNullable: false, isArray: d.IsArray),
        TimeSpanDataField ts => new TimeSpanDataField(ts.Name, ts.TimeSpanFormat, isNullable: false, isArray: d.IsArray),
        _ => new DataField(d.Name, d.ClrType, isNullable: false, isArray: d.IsArray)
    };
}

void PrintSchemaFields(ParquetSchema parquetSchema)
{
    foreach (var field in parquetSchema.Fields)
    {
        if (field is DataField d)
        {
            Console.WriteLine($"{d.Name} {d.SchemaType} {d.ClrType} {d.IsNullable}");
        }
        else
        {
            Console.WriteLine($"{field.Name} {field.SchemaType} {field.IsNullable}");
        }
    }
}
