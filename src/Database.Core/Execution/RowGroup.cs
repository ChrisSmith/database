namespace Database.Core.Execution;

public record RowGroup(List<string> ColumnNames, List<IColumn> Columns)
{
    
}
