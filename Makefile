.PHONY: run
run:
	dotnet run --project src/Cli/Cli.csproj

test:
	dotnet test

# depends on publish_cli.sh first
speedscope:
	dotnet-trace collect --format SpeedScope --duration "00:00:00:45" -- ./bin/cli/Cli -f "src/Database.BenchmarkRunner/Queries/query_02.sql"

speedscope2:
	dotnet-trace collect --format SpeedScope --duration "00:00:00:45" -- ./bin/cli/Cli -f "src/Database.BenchmarkRunner/Queries/query_07.sql"
            
speedscope3:
	dotnet-trace collect --format SpeedScope --duration "00:00:00:45" -- ./bin/cli/Cli "select l_shipdate  from lineitem order by l_extendedprice, l_orderkey limit 3;"
            

benchmark:
	cd src/Database.BenchmarkRunner && DYLD_LIBRARY_PATH=/opt/homebrew/lib dotnet run -c Release
