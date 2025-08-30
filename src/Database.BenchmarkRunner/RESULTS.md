# Benchmark Results

| Runner | [query_01](Queries/query_01.sql) | [query_02](Queries/query_02.sql) | [query_03](Queries/query_03.sql) | [query_04](Queries/query_04.sql) | [query_05](Queries/query_05.sql) | [query_06](Queries/query_06.sql) | [query_07](Queries/query_07.sql) | [query_08](Queries/query_08.sql) | [query_09](Queries/query_09.sql) | [query_10](Queries/query_10.sql) | [query_11](Queries/query_11.sql) | [query_12](Queries/query_12.sql) | [query_13](Queries/query_13.sql) | [query_14](Queries/query_14.sql) | [query_15](Queries/query_15.sql) | [query_16](Queries/query_16.sql) | [query_17](Queries/query_17.sql) | [query_18](Queries/query_18.sql) | [query_19](Queries/query_19.sql) | [query_20](Queries/query_20.sql) | [query_21](Queries/query_21.sql) | [query_22](Queries/query_22.sql) |
|--------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|
| **DuckDb** | 240ms | 40ms | 110ms | 180ms | 200ms | 20ms | 80ms | 270ms | 330ms | 150ms | 40ms | 60ms | 170ms | 10ms | 20ms | 30ms | 220ms | 330ms | 230ms | 50ms | 550ms | 30ms |
| **Spark** | 3,380ms | 640ms | 1,100ms | 1,210ms | 2,020ms | 110ms | 1,370ms | 830ms | 1,670ms | 1,400ms | 330ms | 550ms | 1,380ms | 150ms | ❌ 10ms | 520ms | 1,550ms | 3,890ms | 590ms | 390ms | 4,570ms | 360ms |
| *vs DuckDb* | 14.0x slower | 17.9x slower | 10.1x slower | 6.7x slower | 10.0x slower | 6.3x slower | 16.3x slower | 3.0x slower | 5.0x slower | 9.3x slower | 9.2x slower | 9.8x slower | 8.0x slower | 11.2x slower | ❌ FAILED | 19.3x slower | 7.1x slower | 11.7x slower | 2.6x slower | 8.3x slower | 8.4x slower | 11.9x slower |
| **Sqlite** | 4,940ms | 1,270ms | 6,780ms | ⏰ 0ms | ⏰ 0ms | 720ms | 2,890ms | 13,110ms | 15,130ms | 920ms | 1,090ms | 710ms | 3,980ms | 400ms | 1,100ms | 230ms | ⏰ 0ms | ⏰ 0ms | ⏰ 0ms | ⏰ 0ms | ⏰ 0ms | ⏰ 0ms |
| *vs DuckDb* | 20.4x slower | 35.2x slower | 62.2x slower | ⏰ TIMEOUT | ⏰ TIMEOUT | 39.8x slower | 34.4x slower | 48.0x slower | 45.6x slower | 6.1x slower | 30.3x slower | 12.7x slower | 22.9x slower | 30.5x slower | 64.5x slower | 8.5x slower | ⏰ TIMEOUT | ⏰ TIMEOUT | ⏰ TIMEOUT | ⏰ TIMEOUT | ⏰ TIMEOUT | ⏰ TIMEOUT |
| **Database** | 1,580ms | ⏰ 30,040ms | 2,300ms | ⏰ 30,000ms | 3,650ms | 120ms | 24,760ms | 10,070ms | ⏰ 55,240ms | 880ms | 540ms | 480ms | 6,000ms | 240ms | ❌ 0ms | ❌ 0ms | 6,030ms | 10,010ms | ❌ 0ms | ❌ 0ms | ❌ 0ms | ❌ 0ms |
| *vs DuckDb* | 6.5x slower | ⏰ TIMEOUT | 21.1x slower | ⏰ TIMEOUT | 18.1x slower | 6.4x slower | 294.8x slower | 36.9x slower | ⏰ TIMEOUT | 5.8x slower | 15.1x slower | 8.6x slower | 34.5x slower | 18.4x slower | ❌ FAILED | ❌ FAILED | 27.5x slower | 30.1x slower | ❌ FAILED | ❌ FAILED | ❌ FAILED | ❌ FAILED |

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
- **Average execution time (DuckDb)**: 152ms
- **Average execution time (Spark)**: 1,334ms
- **Average execution time (Sqlite)**: 3,803ms
- **Average execution time (Database)**: 5,127ms

- **Performance ratio**: Spark is ~9x slower than DuckDb on average
- **Performance ratio**: Sqlite is ~25x slower than DuckDb on average
- **Performance ratio**: Database is ~34x slower than DuckDb on average
