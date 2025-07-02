using Database.Core;

namespace Database.Test;

public class ParserTest
{
    [TestCase("*")]
    [TestCase("a")]
    [TestCase("a, b")]
    [TestCase("t.*")]
    [TestCase("t.a")]
    [TestCase("t.a, t.b")]
    [TestCase("a, b, t.a, t.b, t.*, *")]
    public Task Test(string expression)
    {
        var scanner = new Scanner($"SELECT {expression} FROM table t;");
        var tokens = scanner.ScanTokens();

        var parser = new Parser(tokens);
        var result = parser.Parse();
        return Verify(result);
    }
}
