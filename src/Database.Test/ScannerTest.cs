using Database.Core;
using FluentAssertions;
using VerifyTests;

namespace Database.Test;
using static TestUtils;

public class ScannerTest
{
    private static readonly VerifySettings Settings = new();
    static ScannerTest()
    {
        Settings.UseDirectory("Snapshots/Scanner/");
        Settings.AutoVerify();
    }

    [Test]
    public Task Test()
    {
        var scanner = new Scanner("SELECT * FROM table;");
        var tokens = scanner.ScanTokens();
        return Verify(tokens, Settings);
    }

    [Test]
    public Task ColumnIdentifierTest()
    {
        var scanner = new Scanner("SELECT t.a, t.b, t.*, * FROM table t;");
        var tokens = scanner.ScanTokens();
        return Verify(tokens, Settings);
    }

    [Test]
    public Task DistinctTest()
    {
        var scanner = new Scanner("SELECT distinct a, b FROM table t;");
        var tokens = scanner.ScanTokens();
        return Verify(tokens, Settings);
    }

    [Test]
    public Task AllTest()
    {
        var scanner = new Scanner("SELECT all a, b FROM table t;");
        var tokens = scanner.ScanTokens();
        return Verify(tokens, Settings);
    }

    [TestCase("count(1)")]
    [TestCase("count(a)")]
    [TestCase("sum(1)")]
    [TestCase("sum(a)")]
    [TestCase("sum(a), b")]
    public Task Aggregations(string expr)
    {
        var scanner = new Scanner($"SELECT {expr} FROM table t;");
        var tokens = scanner.ScanTokens();
        return Verify(tokens, Settings);
    }


    [TestCase("Id < 100")]
    [TestCase("Id <= 100")]
    [TestCase("Id != 100")]
    [TestCase("Id > 100")]
    [TestCase("Id >= 100")]
    [TestCase("Id = 100")]
    public Task Where(string expr)
    {
        var scanner = new Scanner($"SELECT Id FROM table t where {expr};");
        var tokens = scanner.ScanTokens();

        var parameters = CleanStringForFileName(expr);
        return Verify(tokens, Settings).UseParameters(parameters);
    }

    [TestCase("200 + Id")]
    [TestCase("Id - 100")]
    [TestCase("200 - 1")]
    [TestCase("200 * Id")]
    [TestCase("Id / 100")]
    [TestCase("Id % 100")]
    public Task ScalarMath(string expr)
    {
        var scanner = new Scanner($"SELECT {expr} FROM table;");
        var tokens = scanner.ScanTokens();

        var parameters = CleanStringForFileName(expr);
        return Verify(tokens, Settings).UseParameters(parameters);
    }

    [Test]
    public void Test_ParseException()
    {
        var scanner = new Scanner("SELECT \"foo;");
        var ex = Assert.Throws<ParseException>(() => scanner.ScanTokens());
        ex!.Message.Should().Be("[1:13] Error: Unterminated string.");
    }
}
