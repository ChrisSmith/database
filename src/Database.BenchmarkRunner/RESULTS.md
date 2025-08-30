# Benchmark Results

| Runner | [query_01](Queries/query_01.sql) | [query_02](Queries/query_02.sql) | [query_03](Queries/query_03.sql) | [query_04](Queries/query_04.sql) | [query_05](Queries/query_05.sql) | [query_06](Queries/query_06.sql) | [query_07](Queries/query_07.sql) | [query_08](Queries/query_08.sql) | [query_09](Queries/query_09.sql) | [query_10](Queries/query_10.sql) | [query_11](Queries/query_11.sql) | [query_12](Queries/query_12.sql) | [query_13](Queries/query_13.sql) | [query_14](Queries/query_14.sql) | [query_15](Queries/query_15.sql) | [query_16](Queries/query_16.sql) | [query_17](Queries/query_17.sql) | [query_18](Queries/query_18.sql) | [query_19](Queries/query_19.sql) | [query_20](Queries/query_20.sql) | [query_21](Queries/query_21.sql) | [query_22](Queries/query_22.sql) |
|--------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|
| **DuckDb** | 240ms | 40ms | 110ms | 180ms | 200ms | 20ms | 80ms | 270ms | 330ms | 150ms | 40ms | 60ms | 170ms | 10ms | 20ms | 30ms | 220ms | 330ms | 230ms | 50ms | 530ms | 30ms |
| **Spark** | 3,430ms (14.1x) | 420ms (11.5x) | 1,010ms (9.5x) | 1,180ms (6.6x) | 1,880ms (9.5x) | 100ms (5.3x) | 1,310ms (16.0x) | 740ms (2.7x) | 1,610ms (4.9x) | 1,240ms (8.4x) | 320ms (8.9x) | 530ms (9.5x) | 1,380ms (7.9x) | 130ms (10.2x) | ❌ | 440ms (16.1x) | 1,520ms (6.8x) | 3,850ms (11.8x) | 510ms (2.2x) | 370ms (8.1x) | 4,570ms (8.6x) | 360ms (12.5x) |
| **Sqlite** | 4,940ms (20.3x) | 1,250ms (33.8x) | 6,710ms (63.3x) | ❌ | ❌ | 710ms (39.4x) | 2,890ms (35.2x) | 13,000ms (48.0x) | 15,290ms (46.9x) | 910ms (6.1x) | 1,100ms (30.6x) | 710ms (12.7x) | 4,110ms (23.6x) | 410ms (31.3x) | 1,110ms (61.7x) | 230ms (8.5x) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Database** | 1,570ms (6.5x) | ❌ | 410ms (3.9x) | ❌ | 1,260ms (6.3x) | 120ms (6.6x) | 4,410ms (53.8x) | 240ms (1.1x) | ❌ | 590ms (4.0x) | 540ms (15.0x) | 450ms (8.0x) | 4,340ms (24.9x) | 160ms (12.4x) | ❌ | ❌ | 5,960ms (26.9x) | 5,280ms (16.2x) | ❌ | ❌ | ❌ | ❌ |

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
- **Average execution time (DuckDb)**: 150ms
- **Average execution time (Spark)**: 1,280ms
- **Average execution time (Sqlite)**: 3,810ms
- **Average execution time (Database)**: 1,949ms

- **Performance ratio**: Spark is ~9x slower than DuckDb on average
- **Performance ratio**: Sqlite is ~25x slower than DuckDb on average
- **Performance ratio**: Database is ~13x slower than DuckDb on average
