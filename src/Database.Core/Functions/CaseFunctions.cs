using Database.Core.Catalog;

namespace Database.Core.Functions;

public record CaseWhen(DataType ReturnType) : IFunction;
