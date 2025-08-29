.PHONY: run
run:
	dotnet run --project src/Cli/Cli.csproj

test:
	dotnet test

# depends on publish_cli.sh first
speedscope:
	dotnet-trace collect --format SpeedScope --duration "00:00:00:45" -- ./bin/cli/Cli -f "src/Database.BenchmarkRunner/Queries/query_02.sql"

speedscope2:
	dotnet-trace collect --format SpeedScope --duration "00:00:00:45" -- ./bin/cli/Cli -f "src/Database.BenchmarkRunner/Queries/query_06.sql"
            
benchmark:
	cd src/Database.BenchmarkRunner && dotnet run -c Release
