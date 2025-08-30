# Benchmark Results

| Runner | [query_01](Queries/query_01.sql) | [query_02](Queries/query_02.sql) | [query_03](Queries/query_03.sql) | [query_04](Queries/query_04.sql) | [query_05](Queries/query_05.sql) | [query_06](Queries/query_06.sql) | [query_07](Queries/query_07.sql) | [query_08](Queries/query_08.sql) | [query_09](Queries/query_09.sql) | [query_10](Queries/query_10.sql) | [query_11](Queries/query_11.sql) | [query_12](Queries/query_12.sql) | [query_13](Queries/query_13.sql) | [query_14](Queries/query_14.sql) | [query_15](Queries/query_15.sql) | [query_16](Queries/query_16.sql) | [query_17](Queries/query_17.sql) | [query_18](Queries/query_18.sql) | [query_19](Queries/query_19.sql) | [query_20](Queries/query_20.sql) | [query_21](Queries/query_21.sql) | [query_22](Queries/query_22.sql) |
|--------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|
| **DuckDb** | 240ms | 40ms | 100ms | 180ms | 200ms | 20ms | 80ms | 270ms | 330ms | 150ms | 40ms | 60ms | 180ms | 10ms | 20ms | 30ms | 220ms | 330ms | 230ms | 50ms | 540ms | 30ms |
| **Spark** | 3,310ms | 420ms | 1,010ms | 1,180ms | 1,900ms | 100ms | 1,320ms | 760ms | 1,590ms | 1,370ms | 340ms | 520ms | 1,400ms | 150ms | ❌ 10ms | 480ms | 1,560ms | 3,900ms | 560ms | 360ms | 4,650ms | 400ms |
| *vs DuckDb* | 14.0x slower | 11.6x slower | 9.6x slower | 6.7x slower | 9.7x slower | 5.6x slower | 16.1x slower | 2.9x slower | 4.9x slower | 9.0x slower | 9.6x slower | 9.1x slower | 7.8x slower | 11.8x slower | ❌ FAILED | 17.7x slower | 6.9x slower | 11.8x slower | 2.4x slower | 7.5x slower | 8.6x slower | 14.2x slower |
| **Sqlite** | 4,970ms | 1,260ms | 6,710ms | ⏰ 0ms | ⏰ 0ms | 710ms | 2,890ms | 13,080ms | 16,820ms | 910ms | 1,080ms | 710ms | 3,940ms | 390ms | 1,090ms | 230ms | ⏰ 0ms | ⏰ 0ms | ⏰ 0ms | ⏰ 0ms | ⏰ 0ms | ⏰ 0ms |
| *vs DuckDb* | 21.0x slower | 34.9x slower | 63.9x slower | ⏰ TIMEOUT | ⏰ TIMEOUT | 39.3x slower | 35.2x slower | 49.2x slower | 51.6x slower | 6.0x slower | 30.8x slower | 12.5x slower | 21.9x slower | 30.1x slower | 60.4x slower | 8.4x slower | ⏰ TIMEOUT | ⏰ TIMEOUT | ⏰ TIMEOUT | ⏰ TIMEOUT | ⏰ TIMEOUT | ⏰ TIMEOUT |
| **Database** | 1,580ms | ⏰ 30,340ms | 420ms | ⏰ 30,000ms | 1,270ms | 120ms | 4,410ms | 240ms | ⏰ 45,560ms | 500ms | 630ms | 480ms | 4,290ms | 160ms | ❌ 0ms | ❌ 0ms | 6,260ms | 5,340ms | ❌ 0ms | ❌ 0ms | ❌ 0ms | ❌ 0ms |
| *vs DuckDb* | 6.7x slower | ⏰ TIMEOUT | 4.0x slower | ⏰ TIMEOUT | 6.5x slower | 6.8x slower | 53.8x slower | 1.1x faster | ⏰ TIMEOUT | 3.2x slower | 18.1x slower | 8.5x slower | 23.8x slower | 12.6x slower | ❌ FAILED | ❌ FAILED | 27.8x slower | 16.2x slower | ❌ FAILED | ❌ FAILED | ❌ FAILED | ❌ FAILED |

## Summary

- **DuckDb**: 22/22 queries successful (100.0% success rate)
- **Spark**: 21/22 queries successful (95.5% success rate)
  - 1 failures
- **Sqlite**: 14/22 queries successful (63.6% success rate)
  - 8 timeouts
- **Database**: 13/22 queries successful (59.1% success rate)
  - 6 failures
  - 3 timeouts

### Performance Comparison (Successful Queries Only)
- **Average execution time (DuckDb)**: 151ms
- **Average execution time (Spark)**: 1,298ms
- **Average execution time (Sqlite)**: 3,912ms
- **Average execution time (Database)**: 1,977ms

- **Performance ratio**: Spark is ~9x slower than DuckDb on average
- **Performance ratio**: Sqlite is ~26x slower than DuckDb on average
- **Performance ratio**: Database is ~13x slower than DuckDb on average
