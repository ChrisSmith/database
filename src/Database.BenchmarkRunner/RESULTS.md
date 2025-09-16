# Benchmark Results

| Runner | [query_01](Queries/query_01.sql) | [query_02](Queries/query_02.sql) | [query_03](Queries/query_03.sql) | [query_04](Queries/query_04.sql) | [query_05](Queries/query_05.sql) | [query_06](Queries/query_06.sql) | [query_07](Queries/query_07.sql) | [query_08](Queries/query_08.sql) | [query_09](Queries/query_09.sql) | [query_10](Queries/query_10.sql) | [query_11](Queries/query_11.sql) | [query_12](Queries/query_12.sql) | [query_13](Queries/query_13.sql) | [query_14](Queries/query_14.sql) | [query_15](Queries/query_15.sql) | [query_16](Queries/query_16.sql) | [query_17](Queries/query_17.sql) | [query_18](Queries/query_18.sql) | [query_19](Queries/query_19.sql) | [query_20](Queries/query_20.sql) | [query_21](Queries/query_21.sql) | [query_22](Queries/query_22.sql) |
|--------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|
| **DuckDb** | 320ms | 60ms | 160ms | 220ms | 250ms | 20ms | 90ms | 300ms | 360ms | 170ms | 40ms | 60ms | 200ms | 10ms | 10ms | 30ms | 250ms | 370ms | 260ms | 50ms | 580ms | 30ms |
| **ClickHouse** | 360ms (1.1x) | ❌ | 180ms (1.2x) | ❌ | 360ms (1.5x) | 40ms (1.4x) | 130ms (1.4x) | 400ms (1.3x) | 460ms (1.3x) | 260ms (1.5x) | 50ms (1.3x) | 100ms (1.7x) | 160ms (1.2x) | 20ms (1.5x) | 30ms (2.9x) | 30ms (1.1x) | 340ms (1.3x) | 340ms (1.1x) | 540ms (2.0x) | 60ms (1.2x) | ❌ | ❌ |
| **Database** | 1,720ms (5.4x) | 270ms (4.6x) | 580ms (3.7x) | 2,080ms (9.6x) | 590ms (2.4x) | 130ms (5.3x) | 490ms (5.5x) | 660ms (2.2x) | 2,450ms (6.8x) | 520ms (3.1x) | 60ms (1.6x) | 530ms (8.6x) | 4,970ms (24.5x) | 100ms (7.5x) | 180ms (16.0x) | 180ms (5.9x) | 780ms (3.1x) | 2,310ms (6.3x) | 820ms (3.1x) | ❌ | ❌ | ❌ |
| **Spark** | 4,680ms (14.7x) | 1,280ms (21.4x) | 1,310ms (8.2x) | 1,420ms (6.6x) | 2,260ms (9.1x) | 150ms (6.0x) | 1,570ms (17.6x) | 920ms (3.0x) | 1,840ms (5.1x) | 1,600ms (9.4x) | 430ms (10.5x) | 620ms (10.0x) | 1,460ms (7.2x) | 170ms (12.0x) | 370ms (34.0x) | 570ms (18.5x) | 1,760ms (7.0x) | 4,200ms (11.4x) | 560ms (2.1x) | 430ms (8.7x) | 5,050ms (8.8x) | 380ms (12.3x) |
| **Sqlite** | 5,590ms (17.6x) | 1,420ms (23.6x) | 7,790ms (49.0x) | ❌ | ❌ | 800ms (31.9x) | 3,090ms (34.7x) | 14,530ms (48.1x) | 16,510ms (45.7x) | 970ms (5.7x) | 1,160ms (28.2x) | 760ms (12.2x) | 4,300ms (21.2x) | 420ms (29.6x) | 400ms (36.5x) | 240ms (7.8x) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

## Summary

- **DuckDb**: 22/22 queries successful (100.0% success rate)
- **ClickHouse**: 18/22 queries successful (81.8% success rate)
  - 4 failures
- **Database**: 19/22 queries successful (86.4% success rate)
  - 3 failures
- **Spark**: 22/22 queries successful (100.0% success rate)
- **Sqlite**: 14/22 queries successful (63.6% success rate)
  - 8 timeouts

### Performance Comparison (Successful Queries Only)
- **Average execution time (DuckDb)**: 175ms
- **Average execution time (ClickHouse)**: 215ms
- **Average execution time (Database)**: 1,023ms
- **Average execution time (Spark)**: 1,502ms
- **Average execution time (Sqlite)**: 4,140ms

- **Performance ratio**: ClickHouse is ~1x slower than DuckDb on average
- **Performance ratio**: Database is ~6x slower than DuckDb on average
- **Performance ratio**: Spark is ~9x slower than DuckDb on average
- **Performance ratio**: Sqlite is ~24x slower than DuckDb on average
