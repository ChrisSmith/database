using Database.Core;
using FluentAssertions;
using VerifyTests;

namespace Database.Test;

public class ParserTest
{
    [Test]
    public Task Test()
    {
        var scanner = new Scanner("SELECT * FROM table;");
        var tokens = scanner.ScanTokens();
        return Verify(tokens);
    }
    
    [Test]
    public void Test_ParseException()
    {
        var scanner = new Scanner("SELECT \"foo;");
        var ex = Assert.Throws<ParseException>(() => scanner.ScanTokens());
        ex.Message.Should().Be("[1:13] Error: Unterminated string.");
    }
}
