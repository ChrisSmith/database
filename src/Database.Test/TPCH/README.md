https://duckdb.org/docs/stable/core_extensions/tpch.html

```sql
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS lineitem;
DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS partsupp;
DROP TABLE IF EXISTS region;
DROP TABLE IF EXISTS supplier;

CALL dbgen(sf = 1);

CREATE OR REPLACE TABLE lineitem AS
SELECT *
FROM lineitem
ORDER BY l_shipdate;

EXPORT DATABASE '/Users/chris/src/database/tpch/1' (FORMAT parquet);
```
