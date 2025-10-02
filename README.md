# Database

This project is a learning exercise started during my time at [recurse](https://www.recurse.com/) as a from scratch implementation of an OLAP style database. 

Overview of features:
- SQL parsing and execution
- Supports joins, subqueries, and common table expressions
- Columnar execution engine
- Partial late materialization
- Single node, single threaded execution
- Basic logical and cost based query optimization
- Executes 19/22 TPC-H queries and outperforms some widely used systems on my naive benchmark [RESULTS.md](src/Database.BenchmarkRunner/RESULTS.md)
