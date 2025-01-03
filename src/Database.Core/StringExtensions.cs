namespace Database.Core;

public static class StringExtensions {
    public static string SubstringPos(this string str, int start, int end){
        if(end <= start) {
            throw new ArgumentException($"end {end} <= start {start}");
        }

        var len = end - start;
        return str.Substring(start, len);
    }
}
