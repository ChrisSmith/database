# Benchmark Results

| Runner | [query_01](Queries/query_01.sql) | [query_02](Queries/query_02.sql) | [query_03](Queries/query_03.sql) | [query_04](Queries/query_04.sql) | [query_05](Queries/query_05.sql) | [query_06](Queries/query_06.sql) | [query_07](Queries/query_07.sql) | [query_08](Queries/query_08.sql) | [query_09](Queries/query_09.sql) | [query_10](Queries/query_10.sql) | [query_11](Queries/query_11.sql) | [query_12](Queries/query_12.sql) | [query_13](Queries/query_13.sql) | [query_14](Queries/query_14.sql) | [query_15](Queries/query_15.sql) | [query_16](Queries/query_16.sql) | [query_17](Queries/query_17.sql) | [query_18](Queries/query_18.sql) | [query_19](Queries/query_19.sql) | [query_20](Queries/query_20.sql) | [query_21](Queries/query_21.sql) | [query_22](Queries/query_22.sql) |
|--------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|
| **DuckDb** | 260ms | 40ms | 120ms | 190ms | 210ms | 20ms | 90ms | 280ms | 350ms | 160ms | 40ms | 60ms | 180ms | 10ms | 20ms | 30ms | 240ms | 360ms | 250ms | 50ms | 570ms | 30ms |
| **ClickHouse** | 360ms (1.4x) | ❌ | 170ms (1.5x) | ❌ | 330ms (1.6x) | 40ms (1.8x) | 120ms (1.3x) | 360ms (1.3x) | 440ms (1.2x) | 260ms (1.6x) | 50ms (1.4x) | 100ms (1.7x) | 160ms (1.2x) | 20ms (1.5x) | ❌ | 40ms (1.2x) | 330ms (1.4x) | 330ms (1.1x) | 530ms (2.1x) | 60ms (1.1x) | ❌ | ❌ |
| **Database** | 1,600ms (6.2x) | ❌ | 380ms (3.3x) | ❌ | 1,290ms (6.2x) | 120ms (5.6x) | 3,290ms (37.4x) | 250ms (1.1x) | ❌ | 630ms (3.9x) | 550ms (14.6x) | 460ms (7.7x) | 4,290ms (23.2x) | 170ms (12.9x) | ❌ | ❌ | 6,140ms (25.5x) | 5,470ms (15.2x) | ❌ | ❌ | ❌ | ❌ |
| **Spark** | 3,680ms (14.1x) | 670ms (16.4x) | 1,230ms (10.7x) | 1,340ms (7.1x) | 2,140ms (10.3x) | 130ms (6.2x) | 1,410ms (16.0x) | 830ms (2.9x) | 1,710ms (4.9x) | 1,440ms (9.0x) | 370ms (9.8x) | 560ms (9.5x) | 1,440ms (7.8x) | 190ms (14.8x) | ❌ | 500ms (17.2x) | 1,610ms (6.7x) | 4,070ms (11.3x) | 550ms (2.2x) | 440ms (8.2x) | 4,990ms (8.8x) | 350ms (11.8x) |
| **Sqlite** | 5,320ms (20.5x) | 1,350ms (32.8x) | 7,230ms (62.9x) | ❌ | ❌ | 750ms (35.5x) | 2,990ms (34.0x) | 13,640ms (48.0x) | 17,450ms (50.0x) | 940ms (5.8x) | 1,130ms (29.8x) | 740ms (12.5x) | 4,130ms (22.3x) | 400ms (30.9x) | 1,120ms (62.2x) | 240ms (8.1x) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

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
- **Average execution time (DuckDb)**: 161ms
- **Average execution time (ClickHouse)**: 216ms
- **Average execution time (Database)**: 1,894ms
- **Average execution time (Spark)**: 1,412ms
- **Average execution time (Sqlite)**: 4,100ms

- **Performance ratio**: ClickHouse is ~1x slower than DuckDb on average
- **Performance ratio**: Database is ~12x slower than DuckDb on average
- **Performance ratio**: Spark is ~9x slower than DuckDb on average
- **Performance ratio**: Sqlite is ~25x slower than DuckDb on average
