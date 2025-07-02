namespace Database.Core.Execution;

public record Row(List<object?> Values)
{
    public override int GetHashCode()
    {
        unchecked
        {
            var hash = 17;
            foreach (var value in Values)
            {
                hash = hash * 31 + (value?.GetHashCode() ?? 0);
            }
            return hash;
        }
    }

    public virtual bool Equals(Row? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        if (Values.Count != other.Values.Count) return false;

        for (int i = 0; i < Values.Count; i++)
        {
            var thisValue = Values[i];
            var otherValue = other.Values[i];

            if (thisValue is null && otherValue is null) continue;
            if (thisValue is null || otherValue is null) return false;
            if (!thisValue.Equals(otherValue)) return false;
        }

        return true;
    }
}
