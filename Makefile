.PHONY: run
run:
	dotnet run --project src/Cli/Cli.csproj

test:
	dotnet test

# depends on publish_cli.sh first
# Usage: make speedscope QUERY=18 (or just make speedscope 18)
speedscope:
	@if [ -z "$(QUERY)" ]; then \
		if [ -n "$(word 2,$(MAKECMDGOALS))" ]; then \
			QUERY_NUM=$(word 2,$(MAKECMDGOALS)); \
		else \
			QUERY_NUM=02; \
		fi; \
	else \
		QUERY_NUM=$(QUERY); \
	fi; \
	QUERY_FILE="src/Database.BenchmarkRunner/Queries/query_$${QUERY_NUM}.sql"; \
	if [ ! -f "$$QUERY_FILE" ]; then \
		echo "Error: Query file $$QUERY_FILE does not exist"; \
		exit 1; \
	fi; \
	echo "Running speedscope with $$QUERY_FILE"; \
	dotnet-trace collect --format SpeedScope --duration "00:00:00:45" -- ./bin/cli/Cli -f "$$QUERY_FILE"

# Prevent make from interpreting the query number as a target
%:
	@:

speedscope2:
	dotnet-trace collect --format SpeedScope --duration "00:00:00:45" -- ./bin/cli/Cli -f "src/Database.BenchmarkRunner/Queries/query_07.sql"
            
speedscope3:
	dotnet-trace collect --format SpeedScope --duration "00:00:00:45" -- ./bin/cli/Cli "select l_shipdate  from lineitem order by l_extendedprice, l_orderkey limit 3;"
            

benchmark:
	cd src/Database.BenchmarkRunner && DYLD_LIBRARY_PATH=/opt/homebrew/lib dotnet run -c Release

benchmark-3p:
	cd src/Database.BenchmarkRunner && DYLD_LIBRARY_PATH=/opt/homebrew/lib dotnet run -c Release -- --run-third-party
