namespace Database.Core.Execution;

public interface IColumn{}

public record Column<T>(List<T> Values) : IColumn
{
    
}
