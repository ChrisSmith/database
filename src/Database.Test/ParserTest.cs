using Database.Core;
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
}
