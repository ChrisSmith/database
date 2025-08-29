# Benchmark Results

| Query | DuckDb Status | DuckDb Time | Database Status | Database Time | vs DuckDb |
|-------|---------------|---------------|---------------|---------------|-------------|
| [query_01](Queries/query_01.sql) | ✅ | 253ms | ✅ | 1,662ms | 6.6x slower |
| [query_02](Queries/query_02.sql) | ✅ | 43ms | ⏰ | TIMEOUT (30,200ms) | TIMEOUT |
| [query_03](Queries/query_03.sql) | ✅ | 114ms | ✅ | 2,361ms | 20.7x slower |
| [query_04](Queries/query_04.sql) | ✅ | 185ms | ⏰ | TIMEOUT (30,004ms) | TIMEOUT |
| [query_05](Queries/query_05.sql) | ✅ | 207ms | ✅ | 3,741ms | 18.1x slower |
| [query_06](Queries/query_06.sql) | ✅ | 20ms | ✅ | 112ms | 5.6x slower |
| [query_07](Queries/query_07.sql) | ✅ | 84ms | ✅ | 24,123ms | 287.2x slower |
| [query_08](Queries/query_08.sql) | ✅ | 282ms | ✅ | 9,700ms | 34.4x slower |
| [query_09](Queries/query_09.sql) | ✅ | 338ms | ⏰ | TIMEOUT (56,535ms) | TIMEOUT |
| [query_10](Queries/query_10.sql) | ✅ | 158ms | ✅ | 864ms | 5.5x slower |
| [query_11](Queries/query_11.sql) | ✅ | 38ms | ✅ | 558ms | 14.7x slower |
| [query_12](Queries/query_12.sql) | ✅ | 56ms | ✅ | 470ms | 8.4x slower |
| [query_13](Queries/query_13.sql) | ✅ | 180ms | ✅ | 5,777ms | 32.1x slower |
| [query_14](Queries/query_14.sql) | ✅ | 13ms | ✅ | 173ms | 13.3x slower |
| [query_15](Queries/query_15.sql) | ✅ | 18ms | ❌ | FAILED (0ms) | FAILED |
| [query_16](Queries/query_16.sql) | ✅ | 28ms | ❌ | FAILED (0ms) | FAILED |
| [query_17](Queries/query_17.sql) | ✅ | 235ms | ✅ | 6,061ms | 25.8x slower |
| [query_18](Queries/query_18.sql) | ✅ | 343ms | ✅ | 10,509ms | 30.6x slower |
| [query_19](Queries/query_19.sql) | ✅ | 236ms | ❌ | FAILED (1ms) | FAILED |
| [query_20](Queries/query_20.sql) | ✅ | 48ms | ❌ | FAILED (0ms) | FAILED |
| [query_21](Queries/query_21.sql) | ✅ | 561ms | ❌ | FAILED (0ms) | FAILED |
| [query_22](Queries/query_22.sql) | ✅ | 29ms | ❌ | FAILED (0ms) | FAILED |

## Summary

- **DuckDb**: 22/22 queries successful (100.0% success rate)
- **Database**: 13/22 queries successful (59.1% success rate)
  - 6 failures
  - 3 timeouts

### Performance Comparison (Successful Queries Only)
- **Average execution time (DuckDb)**: 157ms
- **Average execution time (Database)**: 5,085ms

- **Performance ratio**: Database is ~32x slower than DuckDb on average
