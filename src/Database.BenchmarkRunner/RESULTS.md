# Benchmark Results

| Runner | [query_01](Queries/query_01.sql) | [query_02](Queries/query_02.sql) | [query_03](Queries/query_03.sql) | [query_04](Queries/query_04.sql) | [query_05](Queries/query_05.sql) | [query_06](Queries/query_06.sql) | [query_07](Queries/query_07.sql) | [query_08](Queries/query_08.sql) | [query_09](Queries/query_09.sql) | [query_10](Queries/query_10.sql) | [query_11](Queries/query_11.sql) | [query_12](Queries/query_12.sql) | [query_13](Queries/query_13.sql) | [query_14](Queries/query_14.sql) | [query_15](Queries/query_15.sql) | [query_16](Queries/query_16.sql) | [query_17](Queries/query_17.sql) | [query_18](Queries/query_18.sql) | [query_19](Queries/query_19.sql) | [query_20](Queries/query_20.sql) | [query_21](Queries/query_21.sql) | [query_22](Queries/query_22.sql) |
|--------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|
| **DuckDb** | 250ms | 40ms | 110ms | 190ms | 210ms | 20ms | 90ms | 280ms | 340ms | 160ms | 40ms | 60ms | 180ms | 10ms | 10ms | 30ms | 240ms | 350ms | 250ms | 50ms | 570ms | 30ms |
| **ClickHouse** | 350ms (1.4x) | ❌ | 160ms (1.5x) | ❌ | 300ms (1.4x) | 40ms (1.8x) | 110ms (1.3x) | 360ms (1.3x) | 450ms (1.3x) | 260ms (1.6x) | 50ms (1.4x) | 100ms (1.7x) | 160ms (1.2x) | 20ms (1.6x) | 30ms (3.0x) | 30ms (1.1x) | 330ms (1.4x) | 310ms (1.1x) | 530ms (2.1x) | 60ms (1.2x) | ❌ | ❌ |
| **DataFusion** | 400ms (1.6x) | 140ms (3.7x) | 130ms (1.2x) | 170ms (1.1x) | 350ms (1.7x) | 60ms (3.0x) | 340ms (3.9x) | 300ms (1.1x) | 380ms (1.1x) | 240ms (1.5x) | 120ms (3.4x) | 110ms (1.8x) | 210ms (1.1x) | 70ms (5.1x) | 70ms (6.6x) | 100ms (3.5x) | 380ms (1.6x) | 740ms (2.1x) | 240ms (1.0x) | 40ms (1.1x) | 520ms (1.1x) | 90ms (3.1x) |
| **Database** | 1,140ms (4.6x) | 620ms (16.7x) | 560ms (5.1x) | 640ms (3.4x) | 790ms (3.8x) | 80ms (4.4x) | 250ms (2.9x) | 720ms (2.6x) | 1,900ms (5.6x) | 460ms (2.9x) | 70ms (1.9x) | 360ms (6.2x) | 1,500ms (8.1x) | 100ms (7.9x) | 160ms (16.1x) | 210ms (7.3x) | 660ms (2.8x) | 2,090ms (6.0x) | 550ms (2.2x) | ❌ | ❌ | ❌ |
| **Postgres** | 2,430ms (9.7x) | ❌ | 800ms (7.2x) | 800ms (4.3x) | 470ms (2.3x) | 400ms (21.3x) | 540ms (6.3x) | 440ms (1.6x) | 1,140ms (3.4x) | 650ms (4.1x) | 100ms (2.7x) | 650ms (11.2x) | 530ms (2.9x) | 400ms (30.8x) | 390ms (39.4x) | 180ms (6.0x) | ❌ | 2,960ms (8.5x) | 570ms (2.3x) | ❌ | ❌ | 190ms (6.4x) |
| **Spark** | 3,520ms (14.1x) | 400ms (10.7x) | 1,040ms (9.5x) | 1,220ms (6.5x) | 1,990ms (9.6x) | 130ms (6.7x) | 1,410ms (16.4x) | 800ms (2.8x) | 1,590ms (4.7x) | 1,440ms (9.1x) | 350ms (9.4x) | 550ms (9.5x) | 1,340ms (7.2x) | 180ms (14.2x) | 330ms (33.0x) | 480ms (16.6x) | 1,670ms (7.0x) | 4,070ms (11.8x) | 550ms (2.2x) | 390ms (7.8x) | 4,820ms (8.5x) | 350ms (11.5x) |
| **Sqlite** | 5,100ms (20.4x) | 1,310ms (35.5x) | 7,010ms (63.7x) | ❌ | ❌ | 740ms (38.8x) | 2,990ms (34.8x) | 15,240ms (54.0x) | 15,550ms (45.6x) | 940ms (5.9x) | 1,120ms (30.2x) | 730ms (12.6x) | 4,540ms (24.6x) | 400ms (30.9x) | 380ms (37.7x) | 230ms (8.1x) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

## Summary

- **DuckDb**: 22/22 queries successful (100.0% success rate)
- **ClickHouse**: 18/22 queries successful (81.8% success rate)
  - 4 failures
- **DataFusion**: 22/22 queries successful (100.0% success rate)
- **Database**: 19/22 queries successful (86.4% success rate)
  - 3 failures
- **Postgres**: 18/22 queries successful (81.8% success rate)
  - 4 failures
- **Spark**: 22/22 queries successful (100.0% success rate)
- **Sqlite**: 14/22 queries successful (63.6% success rate)
  - 8 timeouts

### Performance Comparison (Successful Queries Only)
- **Average execution time (DuckDb)**: 158ms
- **Average execution time (ClickHouse)**: 202ms
- **Average execution time (DataFusion)**: 236ms
- **Average execution time (Database)**: 678ms
- **Average execution time (Postgres)**: 758ms
- **Average execution time (Spark)**: 1,301ms
- **Average execution time (Sqlite)**: 4,019ms

- **Performance ratio**: ClickHouse is ~1x slower than DuckDb on average
- **Performance ratio**: DataFusion is ~1x slower than DuckDb on average
- **Performance ratio**: Database is ~4x slower than DuckDb on average
- **Performance ratio**: Postgres is ~5x slower than DuckDb on average
- **Performance ratio**: Spark is ~8x slower than DuckDb on average
- **Performance ratio**: Sqlite is ~25x slower than DuckDb on average
