# Benchmark Results

| Runner | [query_01](Queries/query_01.sql) | [query_02](Queries/query_02.sql) | [query_03](Queries/query_03.sql) | [query_04](Queries/query_04.sql) | [query_05](Queries/query_05.sql) | [query_06](Queries/query_06.sql) | [query_07](Queries/query_07.sql) | [query_08](Queries/query_08.sql) | [query_09](Queries/query_09.sql) | [query_10](Queries/query_10.sql) | [query_11](Queries/query_11.sql) | [query_12](Queries/query_12.sql) | [query_13](Queries/query_13.sql) | [query_14](Queries/query_14.sql) | [query_15](Queries/query_15.sql) | [query_16](Queries/query_16.sql) | [query_17](Queries/query_17.sql) | [query_18](Queries/query_18.sql) | [query_19](Queries/query_19.sql) | [query_20](Queries/query_20.sql) | [query_21](Queries/query_21.sql) | [query_22](Queries/query_22.sql) |
|--------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|
| **DuckDb** | 250ms | 40ms | 110ms | 180ms | 210ms | 20ms | 80ms | 280ms | 330ms | 150ms | 40ms | 60ms | 180ms | 10ms | 20ms | 30ms | 240ms | 340ms | 240ms | 50ms | 560ms | 30ms |
| **ClickHouse** | 350ms (1.4x) | ❌ | 160ms (1.5x) | ❌ | 310ms (1.5x) | 40ms (1.8x) | 120ms (1.4x) | 350ms (1.3x) | 440ms (1.3x) | 270ms (1.8x) | 50ms (1.4x) | 100ms (1.7x) | 150ms (1.1x) | 20ms (1.5x) | ❌ | 30ms (1.2x) | 320ms (1.4x) | 310ms (1.1x) | 520ms (2.2x) | 60ms (1.2x) | ❌ | ❌ |
| **Database** | 1,730ms (6.8x) | 290ms (7.2x) | 650ms (5.9x) | 2,350ms (12.8x) | 650ms (3.1x) | 130ms (6.3x) | 370ms (4.4x) | 700ms (2.5x) | 2,110ms (6.4x) | 520ms (3.4x) | 80ms (2.0x) | 440ms (7.7x) | 4,620ms (26.1x) | 100ms (7.4x) | ❌ | 180ms (6.8x) | 760ms (3.2x) | 2,340ms (6.9x) | 670ms (2.8x) | ❌ | ❌ | ❌ |
| **Spark** | 4,300ms (16.9x) | 1,100ms (27.4x) | 1,210ms (11.0x) | 1,360ms (7.4x) | 2,130ms (10.2x) | 140ms (7.2x) | 1,590ms (18.9x) | 880ms (3.2x) | 1,740ms (5.3x) | 1,490ms (9.7x) | 400ms (10.6x) | 580ms (10.1x) | 1,470ms (8.3x) | 210ms (16.5x) | ❌ | 510ms (19.0x) | 1,720ms (7.3x) | 4,090ms (12.0x) | 550ms (2.3x) | 470ms (9.8x) | 4,900ms (8.7x) | 370ms (11.8x) |
| **Sqlite** | 5,310ms (20.9x) | 1,410ms (35.2x) | 7,130ms (64.9x) | ❌ | ❌ | 740ms (36.9x) | 2,990ms (35.6x) | 13,630ms (49.4x) | 17,960ms (54.4x) | 970ms (6.3x) | 1,160ms (30.4x) | 760ms (13.4x) | 4,270ms (24.1x) | 400ms (31.2x) | 1,130ms (66.4x) | 240ms (8.9x) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

## Summary

- **DuckDb**: 22/22 queries successful (100.0% success rate)
- **ClickHouse**: 17/22 queries successful (77.3% success rate)
  - 5 failures
- **Database**: 18/22 queries successful (81.8% success rate)
  - 4 failures
- **Spark**: 21/22 queries successful (95.5% success rate)
  - 1 failures
- **Sqlite**: 14/22 queries successful (63.6% success rate)
  - 8 timeouts

### Performance Comparison (Successful Queries Only)
- **Average execution time (DuckDb)**: 156ms
- **Average execution time (ClickHouse)**: 211ms
- **Average execution time (Database)**: 1,037ms
- **Average execution time (Spark)**: 1,486ms
- **Average execution time (Sqlite)**: 4,150ms

- **Performance ratio**: ClickHouse is ~1x slower than DuckDb on average
- **Performance ratio**: Database is ~7x slower than DuckDb on average
- **Performance ratio**: Spark is ~10x slower than DuckDb on average
- **Performance ratio**: Sqlite is ~27x slower than DuckDb on average
