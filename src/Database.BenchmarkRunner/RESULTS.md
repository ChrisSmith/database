# Benchmark Results

| Runner | [query_01](Queries/query_01.sql) | [query_02](Queries/query_02.sql) | [query_03](Queries/query_03.sql) | [query_04](Queries/query_04.sql) | [query_05](Queries/query_05.sql) | [query_06](Queries/query_06.sql) | [query_07](Queries/query_07.sql) | [query_08](Queries/query_08.sql) | [query_09](Queries/query_09.sql) | [query_10](Queries/query_10.sql) | [query_11](Queries/query_11.sql) | [query_12](Queries/query_12.sql) | [query_13](Queries/query_13.sql) | [query_14](Queries/query_14.sql) | [query_15](Queries/query_15.sql) | [query_16](Queries/query_16.sql) | [query_17](Queries/query_17.sql) | [query_18](Queries/query_18.sql) | [query_19](Queries/query_19.sql) | [query_20](Queries/query_20.sql) | [query_21](Queries/query_21.sql) | [query_22](Queries/query_22.sql) |
|--------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|
| **DuckDb** | 300ms | 50ms | 160ms | 220ms | 250ms | 20ms | 100ms | 310ms | 360ms | 170ms | 40ms | 60ms | 210ms | 10ms | 10ms | 30ms | 260ms | 370ms | 270ms | 50ms | 610ms | 40ms |
| **ClickHouse** | 400ms (1.3x) | ❌ | 170ms (1.1x) | ❌ | 330ms (1.3x) | 40ms (1.8x) | 130ms (1.4x) | 430ms (1.4x) | 520ms (1.4x) | 270ms (1.6x) | 60ms (1.4x) | 100ms (1.7x) | 160ms (1.3x) | 20ms (1.5x) | 30ms (3.1x) | 40ms (1.2x) | 350ms (1.3x) | 330ms (1.1x) | 540ms (2.0x) | 60ms (1.2x) | ❌ | ❌ |
| **Postgres** | 2,690ms (8.8x) | ❌ | 970ms (6.1x) | 880ms (4.0x) | 480ms (2.0x) | 430ms (21.4x) | 570ms (6.0x) | 490ms (1.6x) | 1,260ms (3.5x) | 670ms (3.9x) | 100ms (2.6x) | 680ms (11.0x) | 580ms (2.8x) | 420ms (29.8x) | 400ms (39.9x) | 180ms (6.1x) | ❌ | 3,050ms (8.2x) | 600ms (2.2x) | ❌ | ❌ | 200ms (5.4x) |
| **Database** | 1,660ms (5.5x) | 350ms (6.6x) | 580ms (3.7x) | 680ms (3.1x) | 810ms (3.3x) | 100ms (5.0x) | 260ms (2.8x) | 740ms (2.4x) | 2,000ms (5.5x) | 500ms (2.9x) | 80ms (1.9x) | 420ms (6.8x) | 1,590ms (7.6x) | 110ms (7.9x) | 170ms (17.4x) | 220ms (7.5x) | 720ms (2.8x) | 2,350ms (6.3x) | 730ms (2.7x) | ❌ | ❌ | ❌ |
| **Spark** | 3,960ms (13.0x) | 1,770ms (33.5x) | 2,530ms (15.9x) | 1,760ms (8.0x) | 2,470ms (10.1x) | 120ms (6.2x) | 1,760ms (18.5x) | 980ms (3.1x) | 2,200ms (6.1x) | 1,470ms (8.5x) | 360ms (9.2x) | 610ms (9.8x) | 1,430ms (6.8x) | 150ms (10.9x) | 360ms (36.4x) | 490ms (17.0x) | 1,890ms (7.3x) | 4,330ms (11.7x) | 600ms (2.3x) | 410ms (8.0x) | 5,010ms (8.2x) | 440ms (11.6x) |
| **Sqlite** | 5,630ms (18.5x) | 1,390ms (26.2x) | 7,720ms (48.5x) | ❌ | ❌ | 790ms (39.5x) | 3,080ms (32.5x) | 14,240ms (45.3x) | 17,480ms (48.0x) | 970ms (5.7x) | 1,160ms (29.7x) | 760ms (12.2x) | 4,290ms (20.4x) | 420ms (29.9x) | 390ms (39.2x) | 240ms (8.2x) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

## Summary

- **DuckDb**: 22/22 queries successful (100.0% success rate)
- **ClickHouse**: 18/22 queries successful (81.8% success rate)
  - 4 failures
- **Postgres**: 18/22 queries successful (81.8% success rate)
  - 4 failures
- **Database**: 19/22 queries successful (86.4% success rate)
  - 3 failures
- **Spark**: 22/22 queries successful (100.0% success rate)
- **Sqlite**: 14/22 queries successful (63.6% success rate)
  - 8 timeouts

### Performance Comparison (Successful Queries Only)
- **Average execution time (DuckDb)**: 177ms
- **Average execution time (ClickHouse)**: 221ms
- **Average execution time (Postgres)**: 813ms
- **Average execution time (Database)**: 740ms
- **Average execution time (Spark)**: 1,596ms
- **Average execution time (Sqlite)**: 4,182ms

- **Performance ratio**: ClickHouse is ~1x slower than DuckDb on average
- **Performance ratio**: Postgres is ~5x slower than DuckDb on average
- **Performance ratio**: Database is ~4x slower than DuckDb on average
- **Performance ratio**: Spark is ~9x slower than DuckDb on average
- **Performance ratio**: Sqlite is ~24x slower than DuckDb on average
