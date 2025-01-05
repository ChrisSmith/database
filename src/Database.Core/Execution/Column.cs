namespace Database.Core.Execution;

public interface IColumn{}

public record Column<T>(T[] Values) : IColumn
{
    
}
