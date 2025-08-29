.PHONY: run
run:
	dotnet run --project src/Cli/Cli.csproj

test:
	dotnet test

# depends on publish_cli.sh first
speedscope:
	dotnet-trace collect --format SpeedScope --duration "00:00:00:45" -- ./bin/cli/Cli -f "src/Database.Test/TPCH/Queries/query_02.sql"

speedscope2:
	dotnet-trace collect --format SpeedScope --duration "00:00:00:30" -- ./bin/cli/Cli "\
		select \
            l_orderkey, \
            sum(l_extendedprice*(1-l_discount)) as revenue, \
            o_orderdate, \
            o_shippriority \
            from customer, orders, lineitem \
            where \
                c_mktsegment = 'BUILDING' \
                and c_custkey = o_custkey \
                and l_orderkey = o_orderkey \
                and o_orderdate < date '1995-03-15' \
                group by l_orderkey, o_orderdate, o_shippriority \
                order by revenue desc, o_orderdate \
                limit 10;"
            