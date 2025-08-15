namespace Database.Core.Types;

public enum IntervalType
{
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Year
}

public record Interval(IntervalType Type, int Value)
{
    public DateTime Add(DateTime time)
    {
        return Type switch
        {
            IntervalType.Second => time.AddSeconds(Value),
            IntervalType.Minute => time.AddMinutes(Value),
            IntervalType.Hour => time.AddHours(Value),
            IntervalType.Day => time.AddDays(Value),
            IntervalType.Week => time.AddDays(Value * 7), // hmm
            IntervalType.Month => time.AddMonths(Value),
            IntervalType.Year => time.AddYears(Value),
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    public DateTime Subtract(DateTime time)
    {
        return Type switch
        {
            IntervalType.Second => time.AddSeconds(-Value),
            IntervalType.Minute => time.AddMinutes(-Value),
            IntervalType.Hour => time.AddHours(-Value),
            IntervalType.Day => time.AddDays(-Value),
            IntervalType.Week => time.AddDays(-Value * 7),
            IntervalType.Month => time.AddMonths(-Value),
            IntervalType.Year => time.AddYears(-Value),
            _ => throw new ArgumentOutOfRangeException()
        };
    }
}
