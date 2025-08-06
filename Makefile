.PHONY: run
run:
	dotnet run --project src/Cli/Cli.csproj

test:
	dotnet test

# depends on publish_cli.sh first
speedscope:
	dotnet-trace collect --format SpeedScope -- ./bin/cli/Cli "\
		select \
			l_returnflag, \
			l_linestatus, \
			count(*) as count_order, \
			sum(l_quantity) as sum_qty, \
			sum(l_extendedprice) as sum_base_price, \
			avg(l_quantity) as avg_qty, \
			avg(l_extendedprice) as avg_price, \
			avg(l_discount) as avg_disc, \
			sum(l_extendedprice*(1.0-l_discount)*(1.0+l_tax)) as sum_charge, \
			sum(l_extendedprice*(1.0-l_discount)) as sum_disc_price \
		from lineitem \
		where l_shipdate <= date '1998-12-01' - interval '30' day \
		group by l_returnflag, l_linestatus \
		order by l_returnflag, l_linestatus;"

speedscope2:
	dotnet-trace collect --format SpeedScope -- ./bin/cli/Cli "\
		select \
			sum(l_extendedprice*l_discount) as revenue \
		from lineitem \
		where l_shipdate >= date '1994-01-01' \
		and l_shipdate < date '1994-01-01' + interval '1' year \
		and l_discount between 0.06 and 0.06 + 0.01 \
		and l_quantity < 24;"

speedscope3:
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
            