-- https://duckdb.org/docs/stable/core_extensions/sqlite.html
-- create a new sqlite database with the same data in it
ATTACH 'new_sqlite_database.db' AS sqlite_db (TYPE sqlite);

CREATE TABLE sqlite_db.customer AS SELECT * FROM customer;
CREATE TABLE sqlite_db.lineitem AS SELECT * FROM lineitem;
CREATE TABLE sqlite_db.nation AS SELECT * FROM nation;
CREATE TABLE sqlite_db.orders AS SELECT * FROM orders;
CREATE TABLE sqlite_db.part AS SELECT * FROM part;
CREATE TABLE sqlite_db.partsupp AS SELECT * FROM partsupp;
CREATE TABLE sqlite_db.region AS SELECT * FROM region;
CREATE TABLE sqlite_db.supplier AS SELECT * FROM supplier;
CREATE TABLE sqlite_db.test AS SELECT * FROM test;
