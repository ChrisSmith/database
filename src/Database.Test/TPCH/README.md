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


To run queries against an in-memory only duckdb
```sql
create view customer as select * from read_parquet('./tpch/1/customer2.parquet');
create view lineitem as select * from read_parquet('./tpch/1/lineitem2.parquet');
create view nation as select * from read_parquet('./tpch/1/nation2.parquet');
create view orders as select * from read_parquet('./tpch/1/orders2.parquet');
create view part as select * from read_parquet('./tpch/1/part2.parquet');
create view partsupp as select * from read_parquet('./tpch/1/partsupp2.parquet');
create view region as select * from read_parquet('./tpch/1/region2.parquet');
create view supplier as select * from read_parquet('./tpch/1/supplier2.parquet');
```
