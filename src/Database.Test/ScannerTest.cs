using Database.Core;
using FluentAssertions;
using VerifyTests;

namespace Database.Test;

public class ScannerTest
{
    [Test]
    public Task Test()
    {
        var scanner = new Scanner("SELECT * FROM table;");
        var tokens = scanner.ScanTokens();
        return Verify(tokens);
    }
    
    [Test]
    public Task ColumnIdentifierTest()
    {
        var scanner = new Scanner("SELECT t.a, t.b, t.*, * FROM table t;");
        var tokens = scanner.ScanTokens();
        return Verify(tokens);
    }
    
    [Test]
    public void Test_ParseException()
    {
        var scanner = new Scanner("SELECT \"foo;");
        var ex = Assert.Throws<ParseException>(() => scanner.ScanTokens());
        ex!.Message.Should().Be("[1:13] Error: Unterminated string.");
    }
}
