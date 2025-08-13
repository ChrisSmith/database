using Database.Core.Expressions;

namespace Database.Core.Planner.QueryGraph;

public abstract record Edge(BaseExpression Expression);

public record UnaryEdge(string Relation, BaseExpression Expression) : Edge(Expression)
{

}

public record BinaryEdge(string One, string Two, BaseExpression Expression) : Edge(Expression)
{

}

public record MultiEdge(string[] Relations, BaseExpression Expression) : Edge(Expression)
{

}
