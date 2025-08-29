# Benchmark Results

| Query | DuckDb Status | DuckDb Time | Database Status | Database Time | vs DuckDb |
|-------|---------------|---------------|---------------|---------------|-------------|
| query_00 | ✅ | 258ms | ✅ | 1,651ms | 6.4x slower |
| query_01 | ✅ | 37ms | ⏰ | TIMEOUT (30,275ms) | TIMEOUT |
| query_02 | ✅ | 113ms | ✅ | 2,416ms | 21.4x slower |
| query_03 | ✅ | 188ms | ⏰ | TIMEOUT (30,008ms) | TIMEOUT |
| query_04 | ✅ | 210ms | ✅ | 3,792ms | 18.1x slower |
| query_05 | ✅ | 19ms | ✅ | 108ms | 5.7x slower |
| query_06 | ✅ | 89ms | ✅ | 25,319ms | 284.5x slower |
| query_07 | ✅ | 281ms | ✅ | 10,173ms | 36.2x slower |
| query_08 | ✅ | 341ms | ⏰ | TIMEOUT (56,469ms) | TIMEOUT |
| query_09 | ✅ | 161ms | ✅ | 939ms | 5.8x slower |
| query_10 | ✅ | 37ms | ✅ | 555ms | 15.0x slower |
| query_11 | ✅ | 59ms | ✅ | 456ms | 7.7x slower |
| query_12 | ✅ | 191ms | ✅ | 5,983ms | 31.3x slower |
| query_13 | ✅ | 14ms | ✅ | 233ms | 16.6x slower |
| query_14 | ✅ | 18ms | ❌ | FAILED (0ms) | FAILED |
| query_15 | ✅ | 29ms | ❌ | FAILED (0ms) | FAILED |
| query_16 | ✅ | 240ms | ✅ | 6,512ms | 27.1x slower |
| query_17 | ✅ | 351ms | ✅ | 10,606ms | 30.2x slower |
| query_18 | ✅ | 239ms | ❌ | FAILED (1ms) | FAILED |
| query_19 | ✅ | 49ms | ❌ | FAILED (0ms) | FAILED |
| query_20 | ✅ | 566ms | ❌ | FAILED (0ms) | FAILED |
| query_21 | ✅ | 30ms | ❌ | FAILED (0ms) | FAILED |

## Summary

- **DuckDb**: 22/22 queries successful (100.0% success rate)
- **Database**: 13/22 queries successful (59.1% success rate)
  - 6 failures
  - 3 timeouts

### Performance Comparison (Successful Queries Only)
- **Average execution time (DuckDb)**: 160ms
- **Average execution time (Database)**: 5,287ms

- **Performance ratio**: Database is ~33x slower than DuckDb on average
