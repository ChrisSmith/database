namespace Database.Test;

public static class TestUtils
{
    public static string CleanStringForFileName(string input)
    {
        return input
                .Replace("<", "LESS")
                .Replace(">", "GREATER")
                .Replace("*", "STAR")
                .Replace("/", "SLASH")
            ;
    }
}
