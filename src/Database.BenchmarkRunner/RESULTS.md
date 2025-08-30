# Benchmark Results

| Runner | [query_01](Queries/query_01.sql) | [query_02](Queries/query_02.sql) | [query_03](Queries/query_03.sql) | [query_04](Queries/query_04.sql) | [query_05](Queries/query_05.sql) | [query_06](Queries/query_06.sql) | [query_07](Queries/query_07.sql) | [query_08](Queries/query_08.sql) | [query_09](Queries/query_09.sql) | [query_10](Queries/query_10.sql) | [query_11](Queries/query_11.sql) | [query_12](Queries/query_12.sql) | [query_13](Queries/query_13.sql) | [query_14](Queries/query_14.sql) | [query_15](Queries/query_15.sql) | [query_16](Queries/query_16.sql) | [query_17](Queries/query_17.sql) | [query_18](Queries/query_18.sql) | [query_19](Queries/query_19.sql) | [query_20](Queries/query_20.sql) | [query_21](Queries/query_21.sql) | [query_22](Queries/query_22.sql) |
|--------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|
| **DuckDb** | 240ms | 40ms | 100ms | 180ms | 200ms | 20ms | 80ms | 270ms | 320ms | 150ms | 40ms | 60ms | 170ms | 10ms | 20ms | 30ms | 220ms | 330ms | 230ms | 50ms | 540ms | 30ms |
| **ClickHouse** | 340ms (1.4x) | ❌ | 150ms (1.4x) | ❌ | 280ms (1.4x) | 40ms (1.9x) | 220ms (2.6x) | 360ms (1.3x) | 480ms (1.5x) | 260ms (1.8x) | 50ms (1.4x) | 100ms (1.7x) | 150ms (1.1x) | 20ms (1.5x) | ❌ | 40ms (1.4x) | 320ms (1.4x) | 300ms (1.1x) | 510ms (2.2x) | 60ms (1.2x) | ❌ | ❌ |
| **Database** | 1,610ms (6.7x) | ❌ | 400ms (3.8x) | ❌ | 1,370ms (7.0x) | 130ms (7.1x) | 4,570ms (55.7x) | 260ms (1.0x) | ❌ | 580ms (3.9x) | 660ms (17.9x) | 490ms (8.8x) | 4,430ms (25.8x) | 170ms (13.0x) | ❌ | ❌ | 6,400ms (28.8x) | 5,820ms (17.6x) | ❌ | ❌ | ❌ | ❌ |
| **Spark** | 4,300ms (17.8x) | 1,160ms (32.2x) | 1,280ms (12.2x) | 1,340ms (7.6x) | 2,070ms (10.6x) | 150ms (8.4x) | 1,560ms (19.0x) | 890ms (3.3x) | 1,750ms (5.4x) | 1,480ms (10.1x) | 420ms (11.2x) | 600ms (10.8x) | 1,580ms (9.2x) | 210ms (16.5x) | ❌ | 560ms (20.9x) | 1,890ms (8.5x) | 4,260ms (12.9x) | 540ms (2.4x) | 430ms (8.7x) | 5,070ms (9.3x) | 370ms (11.8x) |
| **Sqlite** | 5,110ms (21.2x) | 1,330ms (37.0x) | 7,210ms (68.6x) | ❌ | ❌ | 760ms (42.3x) | 3,050ms (37.2x) | 14,520ms (54.6x) | 16,150ms (50.3x) | 1,000ms (6.8x) | 1,250ms (33.8x) | 780ms (13.9x) | 4,690ms (27.3x) | 420ms (32.2x) | 1,130ms (66.4x) | 240ms (9.1x) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

## Summary

- **DuckDb**: 22/22 queries successful (100.0% success rate)
- **ClickHouse**: 17/22 queries successful (77.3% success rate)
  - 5 failures
- **Database**: 13/22 queries successful (59.1% success rate)
  - 6 failures
  - 3 timeouts
- **Spark**: 21/22 queries successful (95.5% success rate)
  - 1 failures
- **Sqlite**: 14/22 queries successful (63.6% success rate)
  - 8 timeouts

### Performance Comparison (Successful Queries Only)
- **Average execution time (DuckDb)**: 150ms
- **Average execution time (ClickHouse)**: 214ms
- **Average execution time (Database)**: 2,067ms
- **Average execution time (Spark)**: 1,519ms
- **Average execution time (Sqlite)**: 4,116ms

- **Performance ratio**: ClickHouse is ~1x slower than DuckDb on average
- **Performance ratio**: Database is ~14x slower than DuckDb on average
- **Performance ratio**: Spark is ~10x slower than DuckDb on average
- **Performance ratio**: Sqlite is ~27x slower than DuckDb on average
