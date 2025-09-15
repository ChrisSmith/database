using System.Numerics;
using Database.Core.BufferPool;
using Database.Core.Catalog;
using Database.Core.Execution;
using Database.Core.Expressions;
using Database.Core.Functions;
using Database.Core.Operations;

namespace Database.Core.Planner;

public class CostEstimation(Catalog.Catalog catalog, ParquetPool bufferPool)
{
    public const double EQUI_FILTER_SELECTIVITY = 0.2;

    public CostEstimate Estimate(LogicalPlan plan)
    {
        if (plan is Scan scan)
        {
            if (scan.Filter != null)
            {
                var selectivity = EstimateSelectivity(scan.Filter);
                var rows = (long)(scan.Cardinality * selectivity);
                return new CostEstimate(rows, scan.Cardinality, selectivity);
            }
            return new CostEstimate(scan.Cardinality, scan.Cardinality, 1.0);
        }
        if (plan is Filter filter)
        {
            var selectivity = EstimateSelectivity(filter.Predicate);
            var (inputCardinality, rowsProcessed, _) = Estimate(filter.Input);
            if (filter.Input is Filter)
            {
                // TODO combine stacked filters into a single operator and consider microsoft's sqrt dampener
                selectivity = 1.0;
            }

            var rows = (long)(inputCardinality * selectivity);

            rowsProcessed += rowsProcessed;

            return new CostEstimate(rows, rowsProcessed, selectivity);
        }

        if (plan is JoinSet joinSet)
        {
            var max = joinSet.Relations.Select(r => Estimate(r.Plan)).MaxBy(r => r.OutputCardinality);
            return max!;
        }

        if (plan is Join join)
        {
            var left = Estimate(join.Left);
            var right = Estimate(join.Right);
            var rowsProcessed = left.RowsProcessed + right.RowsProcessed;
            long numRows;
            if (join is { JoinType: JoinType.Inner, Condition: BinaryExpression { Operator: TokenType.EQUAL } })
            {
                var selectivity = EstimateSelectivity(join.Condition);
                numRows = (long)(left.OutputCardinality * selectivity * right.OutputCardinality);
                return new CostEstimate(numRows, rowsProcessed + numRows, selectivity);
            }
            if (join.JoinType == JoinType.Semi || join.JoinType == JoinType.AntiSemi)
            {
                // TODO on the selectivity here
                return left;
            }
            numRows = left.OutputCardinality * right.OutputCardinality;
            return new CostEstimate(numRows, rowsProcessed + numRows, 1.0);

        }

        if (plan is Aggregate aggregate)
        {
            // What is the correct way to estimate this?
            // multiply the NDV of the groupings together?
            //var groupings = aggregate.GroupBy.Select(EstimateExpressionCost).Sum();
            return Estimate(aggregate.Input);
        }

        if (plan is Projection p)
        {
            return Estimate(p.Input);
        }

        if (plan is Sort sort)
        {
            return Estimate(sort.Input);
        }

        if (plan is Limit limit)
        {
            var (inputCardinality, rowsProcessed, _) = Estimate(limit.Input);
            var rows = Math.Min(inputCardinality, limit.Count);
            rowsProcessed += rows;
            return new CostEstimate(rows, rowsProcessed, limit.Count / (double)inputCardinality);
        }

        if (plan is TopNSort top)
        {
            var (inputCardinality, rowsProcessed, _) = Estimate(top.Input);
            var rows = Math.Min(inputCardinality, top.Count);
            rowsProcessed += rows;
            return new CostEstimate(rows, rowsProcessed, top.Count / (double)inputCardinality);
        }

        if (plan is Apply apply)
        {
            return Estimate(apply.Input);
        }

        if (plan is Distinct distinct)
        {
            // TODO distinct estimation
            return Estimate(distinct.Input);
        }

        if (plan is PlanWithSubQueries sub)
        {
            return Estimate(sub.Plan);
        }

        throw new NotImplementedException($"Type of {plan.GetType().Name} not implemented in CostEstimation");
    }

    public double EstimateSelectivity(BaseExpression expr)
    {
        if (expr is BinaryExpression { Operator: TokenType.EQUAL, Left: ColumnExpression l, Right: ColumnExpression r })
        {
            var leftCol = (l.BoundFunction as SelectFunction)?.ColumnRef;
            var rightCol = (r.BoundFunction as SelectFunction)?.ColumnRef;

            if (leftCol != null && rightCol != null)
            {
                var leftNDV = EstimateDistinctValues(leftCol.Value);
                var rightNDV = EstimateDistinctValues(rightCol.Value);
                var nvd = NVD.Estimate(leftNDV, rightNDV);
                return 1.0 / nvd.Value;
            }

            // TODO add cases for binary expressions that bind to a single column a = 2
        }

        return EQUI_FILTER_SELECTIVITY;
    }

    public enum NVDType { Count, TableSize }

    public record NVD(long Value, NVDType Type)
    {
        public static NVD Estimate(NVD a, NVD b)
        {
            // The basic premise is from this thesis, which is used in duckdb
            // https://blobs.duckdb.org/papers/tom-ebergen-msc-thesis-join-order-optimization-with-almost-no-statistics.pdf
            // We assume the NDV of the two columns are drawing from the same real domain
            // If they reported a NDV, take the highest of the columns in the equivalence set
            // If there are no reported NDV, take smallest cardinality of the tables,
            // assuming its the PK in the PK-FK relationship
            if (a.Type == NVDType.Count && b.Type == NVDType.Count)
            {
                return a.Value < b.Value ? a : b;
            }
            if (a.Type == NVDType.Count)
            {
                return a;
            }
            if (b.Type == NVDType.Count)
            {
                return b;
            }
            // Smallest cardinality of the tables
            return a.Value < b.Value ? a : b;
        }
    }

    public NVD EstimateDistinctValues(ColumnRef columnRef)
    {
        if (columnRef.Storage is not ParquetStorage storage)
        {
            return new(1000, NVDType.TableSize); // TODO need to unify the two table structures a bit so we can support readonly memory tables with fixed sizes
        }

        var table = catalog.GetTable(storage.TableId);
        var column = table.Columns.Single(c => c.ColumnRef == columnRef);
        var columnName = column.Name;
        var statsTableRef = table.StatsTable;
        var statsTable = bufferPool.GetMemoryTable(statsTableRef.TableId);

        if (column.DataType == DataType.Int)
        {
            return EstimateNvdFromStats<int>(statsTable, table, columnName);
        }

        if (column.DataType == DataType.Long)
        {
            return EstimateNvdFromStats<long>(statsTable, table, columnName);
        }

        if (column.DataType == DataType.Decimal)
        {
            return EstimateNvdFromStats<decimal>(statsTable, table, columnName);
        }

        if (column.DataType == DataType.Float)
        {
            return EstimateNvdFromStats<float>(statsTable, table, columnName);
        }

        if (column.DataType == DataType.Double)
        {
            return EstimateNvdFromStats<double>(statsTable, table, columnName);
        }

        if (column.DataType == DataType.String)
        {
            return EstimateNvdFromStatsStrings(statsTable, table, columnName);
        }

        if (column.DataType is DataType.Date or DataType.DateTime)
        {
            return EstimateNvdFromStatsDateTime(statsTable, table, columnName, column.DataType);
        }

        return new(table.NumRows, NVDType.TableSize);
    }

    // Notes on the cardinality estimation
    // The parquet distinct counts are not global, so we only know the true value is between
    // [min(ndv, domain_size), min(table_size, domain_size)]
    //
    // Absent global information, we assume full independence between rowGroups to get the
    // upper bound of distinct values
    //
    // There is probably a way to better estimate the NDV size using a probabilistic model
    // Duckdb is using a good-turing estimation
    // https://github.com/duckdb/duckdb/blob/b081b761ec4cb6450ce0d951bc5dc80270b5faf3/src/storage/statistics/distinct_statistics.cpp#L55-L69
    // https://en.wikipedia.org/wiki/Good%E2%80%93Turing_frequency_estimation

    private NVD EstimateNvdFromStatsDateTime(MemoryBasedTable statsTable,
        TableSchema table,
        string columnName,
        DataType columnDataType)
    {
        var distinctCounts = GetStatsValues<int>(statsTable, table.StatsRowGroup, $"{columnName}_$distinct_count");
        var minValues = GetStatsValues<DateTime>(statsTable, table.StatsRowGroup, $"{columnName}_$min");
        var maxValues = GetStatsValues<DateTime>(statsTable, table.StatsRowGroup, $"{columnName}_$max");

        var maxSize = (int)table.NumRows;
        if (distinctCounts != null && distinctCounts.All(i => i > 0)) // values can be int.MinValue
        {
            var sumDistinct = distinctCounts.Sum();
            maxSize = Math.Min(sumDistinct, maxSize);
        }

        if (minValues != null && maxValues != null)
        {
            var min = minValues.Min();
            var max = maxValues.Max();

            if (columnDataType == DataType.Date)
            {
                var domainSize = (int)Math.Ceiling((max! - min!).TotalDays);
                var ndv = Math.Min((int)Convert.ChangeType(domainSize, typeof(int)), maxSize);
                return new(ndv, NVDType.Count);
            }
            else
            {
                var domainSize = (int)Math.Ceiling((max! - min!).TotalSeconds);
                var ndv = Math.Min((int)Convert.ChangeType(domainSize, typeof(int)), maxSize);
                return new(ndv, NVDType.Count);
            }
        }

        return new(maxSize, NVDType.TableSize);
    }

    private NVD EstimateNvdFromStatsStrings(
        MemoryBasedTable statsTable,
        TableSchema table,
        string columnName)
    {
        var distinctCounts = GetStatsValues<int>(statsTable, table.StatsRowGroup, $"{columnName}_$distinct_count");
        var minValues = GetStatsValues<string>(statsTable, table.StatsRowGroup, $"{columnName}_$min");
        var maxValues = GetStatsValues<string>(statsTable, table.StatsRowGroup, $"{columnName}_$max");

        int maxSize = (int)table.NumRows;
        if (distinctCounts != null && distinctCounts.All(i => i > 0)) // values can be int.MinValue
        {
            var sumDistinct = distinctCounts.Sum();
            maxSize = Math.Min(sumDistinct, maxSize);
        }

        if (minValues != null && maxValues != null)
        {
            var domainSize = int.MaxValue;
            var ndv = Math.Min((int)Convert.ChangeType(domainSize, typeof(int)), maxSize);
            return new(ndv, NVDType.Count);
        }

        return new(maxSize, NVDType.TableSize);
    }
    private NVD EstimateNvdFromStats<T>(MemoryBasedTable statsTable, TableSchema table, string columnName) where T : INumber<T>, IMinMaxValue<T>
    {
        var distinctCounts = GetStatsValues<int>(statsTable, table.StatsRowGroup, $"{columnName}_$distinct_count");
        var minValues = GetStatsValues<T>(statsTable, table.StatsRowGroup, $"{columnName}_$min");
        var maxValues = GetStatsValues<T>(statsTable, table.StatsRowGroup, $"{columnName}_$max");

        int maxSize = (int)table.NumRows;
        if (distinctCounts != null && distinctCounts.All(i => i > 0)) // values can be int.MinValue
        {
            var sumDistinct = distinctCounts.Sum();
            maxSize = Math.Min(sumDistinct, maxSize);
        }

        if (minValues != null && maxValues != null)
        {
            var min = minValues.Min();
            var max = maxValues.Max();

            var domainSize = max! - min! + T.One;
            var ndv = Math.Min((int)Convert.ChangeType(domainSize, typeof(int)), maxSize);
            return new(ndv, NVDType.Count);
        }

        return new(maxSize, NVDType.TableSize);
    }

    private T[]? GetStatsValues<T>(MemoryBasedTable statsTable, RowGroup statsRg, string columnName)
    {
        var statsColumn = statsTable.Schema.SingleOrDefault(c => c.Name == columnName);
        if (statsColumn == null)
        {
            return null;
        }
        var statValues = bufferPool.GetColumn(statsColumn.ColumnRef with { RowGroup = statsRg.RowGroupRef.RowGroup });
        if (statValues.Length == 0)
        {
            return null;
        }
        return (T[])statValues.ValuesArray;
    }

    public static long EstimateExpressionCost(BaseExpression expr)
    {
        return expr is ColumnExpression or LiteralExpression ? 1 : 2;
    }

    public static long EstimateExpressionCost(IReadOnlyList<BaseExpression> exprs)
    {
        return exprs.Sum(EstimateExpressionCost);
    }
}

public record CostEstimate(long OutputCardinality, long RowsProcessed, double Selectivity) { }
