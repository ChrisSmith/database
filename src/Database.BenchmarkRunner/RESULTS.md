# Benchmark Results

| Runner | [query_01](Queries/query_01.sql) | [query_02](Queries/query_02.sql) | [query_03](Queries/query_03.sql) | [query_04](Queries/query_04.sql) | [query_05](Queries/query_05.sql) | [query_06](Queries/query_06.sql) | [query_07](Queries/query_07.sql) | [query_08](Queries/query_08.sql) | [query_09](Queries/query_09.sql) | [query_10](Queries/query_10.sql) | [query_11](Queries/query_11.sql) | [query_12](Queries/query_12.sql) | [query_13](Queries/query_13.sql) | [query_14](Queries/query_14.sql) | [query_15](Queries/query_15.sql) | [query_16](Queries/query_16.sql) | [query_17](Queries/query_17.sql) | [query_18](Queries/query_18.sql) | [query_19](Queries/query_19.sql) | [query_20](Queries/query_20.sql) | [query_21](Queries/query_21.sql) | [query_22](Queries/query_22.sql) |
|--------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|
| **DuckDb** | 250ms | 40ms | 110ms | 180ms | 200ms | 20ms | 80ms | 270ms | 330ms | 160ms | 40ms | 60ms | 180ms | 10ms | 20ms | 30ms | 220ms | 330ms | 250ms | 50ms | 560ms | 30ms |
| **Spark** | 3,620ms (14.7x) | 670ms (18.0x) | 1,180ms (10.8x) | 1,330ms (7.2x) | 2,020ms (10.1x) | 130ms (6.8x) | 1,430ms (17.0x) | 870ms (3.2x) | 1,740ms (5.3x) | 1,490ms (9.6x) | 360ms (10.0x) | 540ms (9.4x) | 1,430ms (8.0x) | 160ms (12.6x) | ❌ | 480ms (17.7x) | 1,850ms (8.4x) | 4,240ms (12.7x) | 540ms (2.2x) | 450ms (9.2x) | 4,980ms (8.9x) | 470ms (15.6x) |
| **ClickHouse** | 360ms (1.5x) | ❌ | 160ms (1.5x) | ❌ | 300ms (1.5x) | 40ms (1.9x) | 110ms (1.3x) | 360ms (1.3x) | 460ms (1.4x) | 280ms (1.8x) | 50ms (1.5x) | 100ms (1.8x) | 160ms (1.1x) | 20ms (1.6x) | ❌ | 30ms (1.3x) | 340ms (1.5x) | 320ms (1.0x) | 540ms (2.2x) | 60ms (1.2x) | ❌ | ❌ |
| **Sqlite** | 5,120ms (20.8x) | 1,340ms (36.1x) | 7,270ms (66.7x) | ❌ | ❌ | 740ms (39.0x) | 3,090ms (36.8x) | 14,160ms (52.1x) | 16,090ms (48.6x) | 940ms (6.1x) | 1,120ms (31.2x) | 730ms (12.8x) | 4,230ms (23.6x) | 400ms (31.1x) | 1,120ms (65.8x) | 230ms (8.6x) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Database** | 1,620ms (6.6x) | ❌ | 420ms (3.8x) | ❌ | 1,330ms (6.6x) | 120ms (6.5x) | 4,730ms (56.3x) | 250ms (1.1x) | ❌ | 640ms (4.1x) | 580ms (16.1x) | 470ms (8.3x) | 4,520ms (25.2x) | 170ms (13.2x) | ❌ | ❌ | 6,300ms (28.6x) | 5,860ms (17.5x) | ❌ | ❌ | ❌ | ❌ |

## Summary

- **DuckDb**: 22/22 queries successful (100.0% success rate)
- **Spark**: 21/22 queries successful (95.5% success rate)
  - 1 failures
- **ClickHouse**: 17/22 queries successful (77.3% success rate)
  - 5 failures
- **Sqlite**: 14/22 queries successful (63.6% success rate)
  - 8 timeouts
- **Database**: 13/22 queries successful (59.1% success rate)
  - 6 failures
  - 3 timeouts

### Performance Comparison (Successful Queries Only)
- **Average execution time (DuckDb)**: 154ms
- **Average execution time (Spark)**: 1,427ms
- **Average execution time (ClickHouse)**: 217ms
- **Average execution time (Sqlite)**: 4,041ms
- **Average execution time (Database)**: 2,077ms

- **Performance ratio**: Spark is ~9x slower than DuckDb on average
- **Performance ratio**: ClickHouse is ~1x slower than DuckDb on average
- **Performance ratio**: Sqlite is ~26x slower than DuckDb on average
- **Performance ratio**: Database is ~13x slower than DuckDb on average
