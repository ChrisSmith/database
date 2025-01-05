using System.Data;
using Database.Core.Execution;
using Parquet;
using Parquet.Schema;
using BindingFlags = System.Reflection.BindingFlags;

namespace Database.Core.Operations;

public record FileScan(string Path) : IOperation
{
    private ParquetReader? _reader = null;
    private List<string>? _columnNames = null;
    private int _group = -1;
    private DataField[] _dataFields;

    public RowGroup? Next()
    {
        if (_reader == null)
        {
            _reader = ParquetReader.CreateAsync(Path).GetAwaiter().GetResult();
            
            // Do we want to pass the expected schema from the catalog here and ensure it matches?
            var schema = _reader.Schema;
            var fields = schema.GetDataFields();
            _columnNames = fields.Select(f => f.Name).ToList();
            _dataFields = schema.GetDataFields();
        }
        
        _group++;
        if (_group >= _reader.RowGroupCount)
        {
            return null;
        }
    
        var rg = _reader.OpenRowGroupReader(_group);
        
        var columnValues = new List<IColumn>(_dataFields.Length);

        foreach(var field in _dataFields)
        {
            var column = rg.ReadColumnAsync(field).GetAwaiter().GetResult();
            
            var type = typeof(Column<>).MakeGenericType(field.ClrType);
            var obj = type.GetConstructors().Single().Invoke([column.Data]);
            
            columnValues.Add((IColumn)obj);
        }
        
        return new RowGroup(_columnNames!, columnValues);
    }
}
