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
