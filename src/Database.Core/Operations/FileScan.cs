using Database.Core.Execution;

namespace Database.Core.Operations;

public record FileScan(string Path) : IOperation
{
    private FileStream? _file = null; 
    byte[] buffer = new byte[4096];
    
    public RowGroup? Next()
    {
        if (_file == null)
        {
            _file = new FileStream(Path, FileMode.Open);
        }

        // TODO use parquet here as a real file format
        // Do we want to pass the expected schema from the catalog here and ensure it matches?

        var read = _file.Read(buffer);
        if (read == 0)
        {
            _file.Close();
            return null;
        }
        
        var columnNames = new List<string> {"data"};
        var columnValues = new List<int>(buffer.Length / 4);
        
        for (var i = 0; i < buffer.Length; i += 4)
        {
            columnValues.Add(BitConverter.ToInt32(buffer, i));
        }

        return new RowGroup(columnNames, new List<IColumn>(){new Column<int>(columnValues)});
    }
}
