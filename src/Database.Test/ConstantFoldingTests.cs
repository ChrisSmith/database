using Database.Core;
using Database.Core.Expressions;
using Database.Core.Planner;

namespace Database.Test;

public class ConstantFoldingTests
{
    [TestCase("a like '%foo'", ExpectedResult = "ends_with(a, foo)")]
    [TestCase("a like 'foo%'", ExpectedResult = "starts_with(a, foo)")]
    [TestCase("a like '%foo%'", ExpectedResult = "a like %foo%")]
    [TestCase("a like 'fo%o'", ExpectedResult = "a like fo%o")]
    [TestCase("a", ExpectedResult = "a")]
    public string LikeRewrites(string input)
    {
        return Simplify(input).ToString();
    }

    private BaseExpression Simplify(string expression)
    {
        var expr = CreateExpression(expression);
        return ConstantFolding.Simplify(expr);
    }

    private BaseExpression CreateExpression(string expression)
    {
        var scanner = new Scanner($"SELECT {expression} FROM table t;");
        var tokens = scanner.ScanTokens();

        var parser = new Parser(tokens);
        var result = parser.Parse();
        return ((SelectStatement)result.Statement).SelectList.Expressions[0];
    }
}
