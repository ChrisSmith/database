using System.Reflection;
using Database.Core.JIT;
using FluentAssertions;

namespace Database.Test;

public class ExpressionJitTest
{
    [Test]
    public void Test()
    {
        var method1 = ExpressionJit.FusedMultiplyAdd(debug: true);
        double result = 0;
        try
        {
            double[] left = [1.0d, 1.2d];
            double[] right = [1.0d, 1.2d];

            result = (double)method1.Invoke(null, [left, right])!;
        }
        catch (TargetInvocationException e)
        {
            Assert.Fail((e.InnerException ?? e).ToString());
        }
        result.Should().BeApproximately(2.44d, 1e-6);
    }
}
