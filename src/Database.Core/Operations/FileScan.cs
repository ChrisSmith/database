using System.IO.MemoryMappedFiles;
using System.Reflection;
using Database.Core.Execution;
using Parquet;
using Parquet.Schema;

namespace Database.Core.Operations;

public record FileScan(string Path) : IOperation
{
    private ParquetReader? _reader = null;
    private int _group = -1;
    private DataField[] _dataFields;
    private bool _done = false;

    public RowGroup? Next()
    {
        if (_done)
        {
            return null;
        }

        if (_reader == null)
        {
            // Is this a good idea or a bad one?
            // memory mapping the file takes ~10ms off the runtime (10%) of my simple test query
            // Once we have a page manager maybe we can do this ourselves?
            var file = MemoryMappedFile.CreateFromFile(Path);
            _reader = ParquetReader.CreateAsync(file.CreateViewStream()).GetAwaiter().GetResult();

            // Do we want to pass the expected schema from the catalog here and ensure it matches?
            var schema = _reader.Schema;
            _dataFields = schema.GetDataFields();
        }

        _group++;
        if (_group >= _reader.RowGroupCount)
        {
            _done = true;
            return null;
        }

        var rg = _reader.OpenRowGroupReader(_group);

        var columnValues = new List<IColumn>(_dataFields.Length);

        for (var i = 0; i < _dataFields.Length; i++)
        {
            var field = _dataFields[i];
            var column = rg.ReadColumnAsync(field).GetAwaiter().GetResult();
            var targetType = field.ClrType;
            if (targetType == typeof(decimal))
            {
                targetType = typeof(double);
            }

            Array finalCopy = column.Data;
            if (column.Field.IsNullable && targetType != typeof(string))
            {
                if (column.Data is decimal?[] dec)
                {
                    var copy = new double[column.Data.Length];
                    for (var j = 0; j < column.Data.Length && j < dec.Length; j++)
                    {
                        copy[j] = (double)dec[j]!;
                    }
                    finalCopy = copy;
                }
                else if (column.Data is long?[] decl)
                {
                    var copy = new long[column.Data.Length];
                    for (var j = 0; j < column.Data.Length && j < decl.Length; j++)
                    {
                        copy[j] = (long)decl[j]!;
                    }
                    finalCopy = copy;
                }
                else if (column.Data is int?[] deci)
                {
                    var copy = new int[column.Data.Length];
                    for (var j = 0; j < column.Data.Length && j < deci.Length; j++)
                    {
                        copy[j] = (int)deci[j]!;
                    }
                    finalCopy = copy;
                }
                else if (column.Data is DateTime?[] decdt)
                {
                    var copy = new DateTime[column.Data.Length];
                    for (var j = 0; j < column.Data.Length && j < decdt.Length; j++)
                    {
                        copy[j] = (DateTime)decdt[j]!;
                    }
                    finalCopy = copy;
                }
                else
                {
                    throw new NotImplementedException();
                    finalCopy = Array.CreateInstance(targetType, column.Data.Length);
                    for (var j = 0; j < column.Data.Length; j++)
                    {
                        finalCopy.SetValue(Convert.ChangeType(column.Data.GetValue(j), targetType), j);
                    }
                }
            }

            var obj = ColumnHelper.CreateColumn(targetType, field.Name, i, finalCopy);

            columnValues.Add(obj);
        }

        return new RowGroup(columnValues);
    }
}
