using System.Numerics;
using Database.Core.Execution;
using Database.Core.Expressions;

namespace Database.Core.Operations;

public class SortOperator(IOperation source, List<OrderingExpression> orderExpressions) : IOperation
{
    bool _done = false;

    public RowGroup? Next()
    {
        if (_done)
        {
            return null;
        }

        var allRows = new List<Row>();
        var next = source.Next();
        while (next != null)
        {
            allRows.AddRange(next.MaterializeRows());
            next = source.Next();
        }
        _done = true;

        // TODO pull these out of the materialized expressions
        var columns = new List<int>() { 0, 1 };
        var asArray = allRows.ToArray();
        Array.Sort(asArray, new RowComparer(columns));

        return RowGroup.FromRows(asArray);
    }

    public class RowComparer(List<int> indexes) : IComparer<Row>
    {
        public int Compare(Row x, Row y)
        {
            for (var i = 0; i < indexes.Count; i++)
            {
                var index = indexes[i];
                var xVal = (IComparable)x.Values[index]!;
                var yVal = (IComparable)y.Values[index]!;

                var res = xVal.CompareTo(yVal);
                if (res != 0)
                {
                    return res;
                }
            }
            return 0;
        }
    }
}
