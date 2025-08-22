using Database.Core;

namespace Database.Test;

using static TestUtils;
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
    [TestCase("sum(a + b)")]
    [TestCase("sum(a + b * 1)")]
    [TestCase("sum(b * 1 + a)")]
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
    [TestCase("Id <= 100")]
    [TestCase("100 > Id")]
    [TestCase("100 >= Id")]
    [TestCase("100 = Id")]
    [TestCase("100 != Id")]
    [TestCase("Id between 10 and 100")]
    [TestCase("Id not between 10 and 100")]
    [TestCase("Foo like '%bar%'")]
    [TestCase("Foo not like '%bar%'")]
    public Task Where(string expr)
    {
        var scanner = new Scanner($"SELECT Id FROM table t where {expr};");
        var tokens = scanner.ScanTokens();

        var parser = new Parser(tokens);
        var result = parser.Parse();

        var parameters = CleanStringForFileName(expr);
        return Verify(result, Settings).UseParameters(parameters);
    }

    // https://www.sqlite.org/lang_expr.html
    [TestCase("200 + Id")]
    [TestCase("Id + 100")]
    [TestCase("200 + 1")]
    [TestCase("200 - Id")]
    [TestCase("Id - 100")]
    [TestCase("200 - 1")]
    [TestCase("200 * Id")]
    [TestCase("Id * 100")]
    [TestCase("200 * 1")]
    [TestCase("200 / Id")]
    [TestCase("Id / 100")]
    [TestCase("200 / 1")]
    [TestCase("200 % Id")]
    [TestCase("Id % 100")]
    [TestCase("200 % 1")]
    public Task ScalarMath(string expr)
    {
        var scanner = new Scanner($"SELECT {expr} FROM table;");
        var tokens = scanner.ScanTokens();

        var parser = new Parser(tokens);
        var result = parser.Parse();


        var parameters = CleanStringForFileName(expr);
        return Verify(result, Settings).UseParameters(parameters);
    }

    [TestCase(@"
            FROM table t1
            join table t2 on t1.Id = t2.Id
            where t1.Id < 100
            "
    )]
    [TestCase(@"
            FROM table t1, table t2
            where t1.Id = t2.Id and t1.Id < 100
            "
    )]
    public Task Joins(string join)
    {
        var scanner = new Scanner($"SELECT t1.Id, t2.Id {join};");
        var tokens = scanner.ScanTokens();

        var parser = new Parser(tokens);
        var result = parser.Parse();

        var parameters = CleanStringForFileName(join);
        return Verify(result, Settings).UseParameters(parameters);
    }
}
