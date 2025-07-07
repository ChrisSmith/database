using Database.Core;

namespace Database.Test;

public class ParserTest
{
    private static readonly VerifySettings Settings = new();
    static ParserTest()
    {
        Settings.UseDirectory("Snapshots/Parser/");
        Settings.AutoVerify();
    }

    [TestCase("*")]
    [TestCase("a")]
    [TestCase("a, b")]
    [TestCase("t.*")]
    [TestCase("t.a")]
    [TestCase("t.a, t.b")]
    [TestCase("a, b, t.a, t.b, t.*, *")]
    [TestCase("distinct a, b")]
    [TestCase("all a, b")]
    public Task Test(string expression)
    {
        var scanner = new Scanner($"SELECT {expression} FROM table t;");
        var tokens = scanner.ScanTokens();

        var parser = new Parser(tokens);
        var result = parser.Parse();
        return Verify(result, Settings);
    }

    [TestCase("count(1)")]
    [TestCase("count(a)")]
    [TestCase("sum(1)")]
    [TestCase("sum(a)")]
    [TestCase("sum(a), count(a)")]
    public Task Aggregations(string expression)
    {
        var scanner = new Scanner($"SELECT {expression} FROM table t;");
        var tokens = scanner.ScanTokens();

        var parser = new Parser(tokens);
        var result = parser.Parse();
        return Verify(result, Settings);
    }

    [TestCase("Id < 100")]
    public Task Where(string expr)
    {
        var scanner = new Scanner($"SELECT Id FROM table t where {expr};");
        var tokens = scanner.ScanTokens();

        var parser = new Parser(tokens);
        var result = parser.Parse();

        return Verify(result, Settings);
    }
}
